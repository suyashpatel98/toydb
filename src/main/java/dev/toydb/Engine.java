package dev.toydb;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Engine implements AutoCloseable {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern CREATE = Pattern.compile(
            "(?is)^CREATE\\s+TABLE\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*\\((.*)\\)$");
    private static final Pattern INSERT = Pattern.compile(
            "(?is)^INSERT\\s+INTO\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*(?:\\(([^)]*)\\))?\\s+VALUES\\s*\\((.*)\\)$");
    private static final Pattern SELECT_ALL = Pattern.compile(
            "(?is)^SELECT\\s+\\*\\s+FROM\\s+([A-Za-z_][A-Za-z0-9_]*)(?:\\s+WHERE\\s+(.+))?$");
    private static final Pattern UPDATE = Pattern.compile(
            "(?is)^UPDATE\\s+([A-Za-z_][A-Za-z0-9_]*)\\s+SET\\s+(.+)$");
    private static final Pattern DELETE = Pattern.compile(
            "(?is)^DELETE\\s+FROM\\s+([A-Za-z_][A-Za-z0-9_]*)(?:\\s+WHERE\\s+(.+))?$");
    private static final Pattern COMPARISON = Pattern.compile(
            "(?is)^([A-Za-z_][A-Za-z0-9_]*)\\s*(<=|>=|<>|!=|=|<|>)\\s*(.+)$");

    private final Path directory;
    private final Map<String, Table> tables = new LinkedHashMap<>();
    private final Semaphore transactionGate = new Semaphore(1, true);
    private final LsmStorage storage;
    private boolean closed;

    Engine(Path directory, int memtableEntries) throws IOException {
        this.directory = directory;
        LsmStorage openedStorage = new LsmStorage(directory, memtableEntries);
        try {
            tables.putAll(openedStorage.load());
        } catch (IOException | RuntimeException exception) {
            try {
                openedStorage.close();
            } catch (IOException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
        storage = openedStorage;
    }

    SqlResult execute(String rawSql) {
        transactionGate.acquireUninterruptibly();
        try {
            synchronized (this) {
                ensureOpen();
                Map<String, TreeMap<Key, List<Object>>> before = snapshotRows();
                SqlResult result = executeLocked(rawSql);
                try {
                    storage.commit(diff(before));
                } catch (IOException exception) {
                    restore(before);
                    throw new DbException("failed to commit statement", exception);
                }
                return result;
            }
        } finally {
            transactionGate.release();
        }
    }

    Transaction begin() {
        transactionGate.acquireUninterruptibly();
        synchronized (this) {
            try {
                ensureOpen();
                return new Transaction(snapshotRows());
            } catch (RuntimeException exception) {
                transactionGate.release();
                throw exception;
            }
        }
    }

    SqlResult execute(Transaction transaction, String rawSql) {
        if (!transaction.active()) {
            throw new DbException("transaction is no longer active");
        }
        if (clean(rawSql).toUpperCase(Locale.ROOT).startsWith("CREATE TABLE ")) {
            throw new DbException("CREATE TABLE is not supported inside a transaction");
        }
        synchronized (this) {
            ensureOpen();
            return executeLocked(rawSql);
        }
    }

    void commit(Transaction transaction) {
        if (!transaction.deactivate()) {
            throw new DbException("transaction is no longer active");
        }
        try {
            synchronized (this) {
                try {
                    storage.commit(diff(transaction.before()));
                } catch (IOException exception) {
                    restore(transaction.before());
                    throw new DbException("failed to commit transaction", exception);
                }
            }
        } finally {
            transactionGate.release();
        }
    }

    void rollback(Transaction transaction) {
        if (!transaction.deactivate()) {
            throw new DbException("transaction is no longer active");
        }
        try {
            synchronized (this) {
                restore(transaction.before());
            }
        } finally {
            transactionGate.release();
        }
    }

    private SqlResult executeLocked(String rawSql) {
        String sql = clean(rawSql);
        Matcher matcher = CREATE.matcher(sql);
        if (matcher.matches()) {
            return createTable(matcher.group(1), matcher.group(2));
        }
        matcher = INSERT.matcher(sql);
        if (matcher.matches()) {
            return insert(matcher.group(1), matcher.group(2), matcher.group(3));
        }
        matcher = SELECT_ALL.matcher(sql);
        if (matcher.matches()) {
            return selectAll(matcher.group(1), matcher.group(2));
        }
        matcher = UPDATE.matcher(sql);
        if (matcher.matches()) {
            ClauseParts clauses = splitWhereClause(matcher.group(2));
            return update(matcher.group(1), clauses.statement(), clauses.condition());
        }
        matcher = DELETE.matcher(sql);
        if (matcher.matches()) {
            return delete(matcher.group(1), matcher.group(2));
        }
        throw new DbException("unsupported or invalid SQL: " + rawSql);
    }

    private SqlResult createTable(String rawName, String definitions) {
        String tableName = identifier(rawName);
        if (tables.containsKey(tableName)) {
            throw new DbException("table already exists: " + tableName);
        }
        List<Column> columns = new ArrayList<>();
        int primaryKey = -1;
        for (String definition : splitCommaSeparated(definitions)) {
            String[] parts = definition.trim().split("\\s+");
            if (parts.length < 2 || parts.length == 3 || parts.length > 4
                    || (parts.length == 4
                    && !(parts[2].equalsIgnoreCase("PRIMARY")
                    && parts[3].equalsIgnoreCase("KEY")))) {
                throw new DbException("invalid column definition: " + definition.trim());
            }
            String columnName = identifier(parts[0]);
            if (columns.stream().anyMatch(column -> column.name().equals(columnName))) {
                throw new DbException("duplicate column: " + columnName);
            }
            ValueType type = ValueType.parse(parts[1]);
            if (parts.length == 4) {
                if (primaryKey >= 0) {
                    throw new DbException("a table must have exactly one primary key");
                }
                primaryKey = columns.size();
            }
            columns.add(new Column(columnName, type));
        }
        if (columns.isEmpty() || primaryKey < 0) {
            throw new DbException("a table must have exactly one primary key");
        }
        Table table = new Table(tableName, columns, primaryKey);
        try {
            storage.createTable(table);
        } catch (IOException exception) {
            throw new DbException("failed to persist table schema", exception);
        }
        tables.put(tableName, table);
        return SqlResult.update(0);
    }

    private SqlResult insert(String rawName, String rawColumns, String rawValues) {
        Table table = table(rawName);
        List<String> valueTokens = splitCommaSeparated(rawValues);
        List<String> insertColumns = rawColumns == null
                ? table.columnNames()
                : splitCommaSeparated(rawColumns).stream().map(String::trim).map(Engine::identifier).toList();
        if (insertColumns.size() != valueTokens.size()) {
            throw new DbException("column count does not match value count");
        }

        List<Object> row = new ArrayList<>();
        for (int i = 0; i < table.columns().size(); i++) {
            row.add(null);
        }
        for (int i = 0; i < insertColumns.size(); i++) {
            int index = table.columnIndex(insertColumns.get(i));
            if (row.get(index) != null) {
                throw new DbException("duplicate insert column: " + insertColumns.get(i));
            }
            row.set(index, table.columns().get(index).type().parseLiteral(valueTokens.get(i)));
        }
        if (row.stream().anyMatch(value -> value == null)) {
            throw new DbException("all columns require a value");
        }
        Key key = Key.of(row.get(table.primaryKeyIndex()));
        if (table.rows().putIfAbsent(key, List.copyOf(row)) != null) {
            throw new DbException("duplicate primary key: " + key.value());
        }
        return SqlResult.update(1);
    }

    private SqlResult selectAll(String rawName, String rawCondition) {
        Table table = table(rawName);
        RowPredicate predicate = parsePredicate(table, rawCondition);
        List<List<Object>> rows = table.rows().values().stream().filter(predicate::test).toList();
        return SqlResult.query(table.columnNames(), rows);
    }

    private SqlResult update(String rawName, String rawAssignments, String rawCondition) {
        Table table = table(rawName);
        Map<Integer, Object> assignments = new LinkedHashMap<>();
        for (String text : splitCommaSeparated(rawAssignments)) {
            int equals = indexOfUnquoted(text, '=');
            if (equals < 1) {
                throw new DbException("invalid assignment: " + text);
            }
            int columnIndex = table.columnIndex(identifier(text.substring(0, equals)));
            if (assignments.containsKey(columnIndex)) {
                throw new DbException("duplicate assignment: " + table.columns().get(columnIndex).name());
            }
            Object value = table.columns().get(columnIndex).type().parseLiteral(text.substring(equals + 1));
            assignments.put(columnIndex, value);
        }
        if (assignments.isEmpty()) {
            throw new DbException("UPDATE requires at least one assignment");
        }

        RowPredicate predicate = parsePredicate(table, rawCondition);
        TreeMap<Key, List<Object>> changed = new TreeMap<>(table.rows());
        int count = 0;
        for (Map.Entry<Key, List<Object>> entry : table.rows().entrySet()) {
            if (!predicate.test(entry.getValue())) continue;
            List<Object> row = new ArrayList<>(entry.getValue());
            assignments.forEach(row::set);
            Key newKey = Key.of(row.get(table.primaryKeyIndex()));
            changed.remove(entry.getKey());
            if (!newKey.equals(entry.getKey()) && changed.containsKey(newKey)) {
                throw new DbException("duplicate primary key: " + newKey.value());
            }
            changed.put(newKey, List.copyOf(row));
            count++;
        }
        table.rows().clear();
        table.rows().putAll(changed);
        return SqlResult.update(count);
    }

    private SqlResult delete(String rawName, String rawCondition) {
        Table table = table(rawName);
        RowPredicate predicate = parsePredicate(table, rawCondition);
        int before = table.rows().size();
        table.rows().entrySet().removeIf(entry -> predicate.test(entry.getValue()));
        return SqlResult.update(before - table.rows().size());
    }

    private RowPredicate parsePredicate(Table table, String rawCondition) {
        if (rawCondition == null) return row -> true;
        List<RowPredicate> predicates = new ArrayList<>();
        for (String condition : splitOnAnd(rawCondition)) {
            Matcher matcher = COMPARISON.matcher(condition.trim());
            if (!matcher.matches()) {
                throw new DbException("invalid WHERE condition: " + condition.trim());
            }
            int columnIndex = table.columnIndex(identifier(matcher.group(1)));
            String operator = matcher.group(2);
            Object expected = table.columns().get(columnIndex).type().parseLiteral(matcher.group(3));
            predicates.add(row -> compare(row.get(columnIndex), expected, operator));
        }
        return row -> predicates.stream().allMatch(predicate -> predicate.test(row));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static boolean compare(Object actual, Object expected, String operator) {
        int comparison = ((Comparable) actual).compareTo(expected);
        return switch (operator) {
            case "=" -> comparison == 0;
            case "!=", "<>" -> comparison != 0;
            case "<" -> comparison < 0;
            case "<=" -> comparison <= 0;
            case ">" -> comparison > 0;
            case ">=" -> comparison >= 0;
            default -> throw new DbException("unsupported comparison operator: " + operator);
        };
    }

    private Table table(String rawName) {
        String name = identifier(rawName);
        Table table = tables.get(name);
        if (table == null) {
            throw new DbException("table does not exist: " + name);
        }
        return table;
    }

    private static String clean(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new DbException("SQL must not be empty");
        }
        String cleaned = sql.trim();
        if (cleaned.endsWith(";")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1).trim();
        }
        return cleaned;
    }

    static String identifier(String value) {
        String identifier = value.trim();
        if (!IDENTIFIER.matcher(identifier).matches()) {
            throw new DbException("invalid identifier: " + identifier);
        }
        return identifier.toLowerCase(Locale.ROOT);
    }

    static List<String> splitCommaSeparated(String text) {
        List<String> parts = new ArrayList<>();
        StringBuilder part = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' && quoted && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                part.append("''");
                i++;
            } else if (c == '\'') {
                quoted = !quoted;
                part.append(c);
            } else if (c == ',' && !quoted) {
                parts.add(part.toString().trim());
                part.setLength(0);
            } else {
                part.append(c);
            }
        }
        if (quoted) {
            throw new DbException("unterminated string literal");
        }
        parts.add(part.toString().trim());
        return parts;
    }

    private static List<String> splitOnAnd(String text) {
        List<String> parts = new ArrayList<>();
        StringBuilder part = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' && quoted && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                part.append("''");
                i++;
            } else if (c == '\'') {
                quoted = !quoted;
                part.append(c);
            } else if (!quoted && i + 3 <= text.length()
                    && text.regionMatches(true, i, "AND", 0, 3)
                    && (i == 0 || Character.isWhitespace(text.charAt(i - 1)))
                    && (i + 3 == text.length() || Character.isWhitespace(text.charAt(i + 3)))) {
                parts.add(part.toString().trim());
                part.setLength(0);
                i += 2;
            } else {
                part.append(c);
            }
        }
        if (part.toString().isBlank()) {
            throw new DbException("empty WHERE condition");
        }
        parts.add(part.toString().trim());
        return parts;
    }

    private static int indexOfUnquoted(String text, char wanted) {
        boolean quoted = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' && quoted && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                i++;
            } else if (c == '\'') {
                quoted = !quoted;
            } else if (c == wanted && !quoted) {
                return i;
            }
        }
        return -1;
    }

    private static ClauseParts splitWhereClause(String text) {
        int where = indexOfKeywordUnquoted(text, "WHERE");
        if (where < 0) return new ClauseParts(text.trim(), null);
        String statement = text.substring(0, where).trim();
        String condition = text.substring(where + "WHERE".length()).trim();
        if (statement.isEmpty() || condition.isEmpty()) {
            throw new DbException("UPDATE has an empty SET or WHERE clause");
        }
        return new ClauseParts(statement, condition);
    }

    private static int indexOfKeywordUnquoted(String text, String keyword) {
        boolean quoted = false;
        for (int i = 0; i + keyword.length() <= text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' && quoted && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                i++;
            } else if (c == '\'') {
                quoted = !quoted;
            } else if (!quoted && text.regionMatches(true, i, keyword, 0, keyword.length())
                    && i > 0 && Character.isWhitespace(text.charAt(i - 1))
                    && i + keyword.length() < text.length()
                    && Character.isWhitespace(text.charAt(i + keyword.length()))) {
                return i;
            }
        }
        return -1;
    }

    private Map<String, TreeMap<Key, List<Object>>> snapshotRows() {
        Map<String, TreeMap<Key, List<Object>>> snapshot = new LinkedHashMap<>();
        tables.forEach((name, table) -> snapshot.put(name, new TreeMap<>(table.rows())));
        return snapshot;
    }

    private void restore(Map<String, TreeMap<Key, List<Object>>> snapshot) {
        snapshot.forEach((name, rows) -> {
            Table table = tables.get(name);
            if (table != null) {
                table.rows().clear();
                table.rows().putAll(rows);
            }
        });
    }

    private List<Mutation> diff(Map<String, TreeMap<Key, List<Object>>> before) {
        List<Mutation> mutations = new ArrayList<>();
        for (Map.Entry<String, TreeMap<Key, List<Object>>> entry : before.entrySet()) {
            String tableName = entry.getKey();
            Table table = tables.get(tableName);
            if (table == null) continue;
            TreeSet<Key> keys = new TreeSet<>(entry.getValue().keySet());
            keys.addAll(table.rows().keySet());
            for (Key key : keys) {
                List<Object> oldRow = entry.getValue().get(key);
                List<Object> newRow = table.rows().get(key);
                if (!Objects.equals(oldRow, newRow)) {
                    mutations.add(new Mutation(tableName, key, newRow));
                }
            }
        }
        return mutations;
    }

    private void ensureOpen() {
        if (closed) throw new DbException("database is closed");
    }

    @Override
    public void close() throws IOException {
        transactionGate.acquireUninterruptibly();
        try {
            synchronized (this) {
                if (closed) return;
                closed = true;
                storage.close();
            }
        } finally {
            transactionGate.release();
        }
    }
}

enum ValueType {
    INT {
        @Override Object parseLiteral(String token) {
            try {
                return Long.parseLong(token.trim());
            } catch (NumberFormatException exception) {
                throw new DbException("invalid INT literal: " + token, exception);
            }
        }
    },
    TEXT {
        @Override Object parseLiteral(String token) {
            String value = token.trim();
            if (value.length() < 2 || value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'') {
                throw new DbException("invalid TEXT literal: " + token);
            }
            String parsed = value.substring(1, value.length() - 1).replace("''", "'");
            if (parsed.getBytes(StandardCharsets.UTF_8).length > ValueCodec.MAX_STRING_BYTES) {
                throw new DbException("TEXT value exceeds the 16 MiB storage limit");
            }
            return parsed;
        }
    },
    BOOLEAN {
        @Override Object parseLiteral(String token) {
            if (token.trim().equalsIgnoreCase("true")) return true;
            if (token.trim().equalsIgnoreCase("false")) return false;
            throw new DbException("invalid BOOLEAN literal: " + token);
        }
    };

    abstract Object parseLiteral(String token);

    boolean accepts(Object value) {
        return switch (this) {
            case INT -> value instanceof Long;
            case TEXT -> value instanceof String;
            case BOOLEAN -> value instanceof Boolean;
        };
    }

    static ValueType parse(String text) {
        try {
            return valueOf(text.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new DbException("unsupported type: " + text, exception);
        }
    }
}

record Column(String name, ValueType type) {}

record Key(Object value) implements Comparable<Key> {
    static Key of(Object value) {
        return new Key(value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public int compareTo(Key other) {
        if (!value.getClass().equals(other.value.getClass())) {
            return value.getClass().getName().compareTo(other.value.getClass().getName());
        }
        return ((Comparable) value).compareTo(other.value);
    }
}

record Table(String name, List<Column> columns, int primaryKeyIndex, TreeMap<Key, List<Object>> rows) {
    Table(String name, List<Column> columns, int primaryKeyIndex) {
        this(name, List.copyOf(columns), primaryKeyIndex, new TreeMap<>());
    }

    List<String> columnNames() {
        return columns.stream().map(Column::name).toList();
    }

    int columnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) return i;
        }
        throw new DbException("unknown column: " + name);
    }
}

@FunctionalInterface
interface RowPredicate {
    boolean test(List<Object> row);
}

record ClauseParts(String statement, String condition) {}

final class Transaction {
    private final Map<String, TreeMap<Key, List<Object>>> before;
    private boolean active = true;

    Transaction(Map<String, TreeMap<Key, List<Object>>> before) {
        this.before = before;
    }

    Map<String, TreeMap<Key, List<Object>>> before() {
        return before;
    }

    synchronized boolean active() {
        return active;
    }

    synchronized boolean deactivate() {
        if (!active) return false;
        active = false;
        return true;
    }
}
