package dev.toydb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/** Minimal log-structured merge storage: WAL -> memtable -> immutable SSTables. */
final class LsmStorage implements Closeable {
    static final int DEFAULT_MEMTABLE_ENTRIES = 1_024;

    private static final int CATALOG_MAGIC = 0x54444243; // TDBC
    private static final int SSTABLE_MAGIC = 0x54444253; // TDBS
    private static final int FORMAT_VERSION = 1;
    private static final int COMPACTION_FAN_IN = 4;
    private static final Pattern SSTABLE_NAME = Pattern.compile("sst-(\\d+)\\.sst");

    private final Path schemaDirectory;
    private final Path dataDirectory;
    private final int memtableEntries;
    private final DirectoryLock directoryLock;
    private final Wal wal;
    private final Map<String, TreeMap<Key, Mutation>> memtables = new LinkedHashMap<>();
    private final Map<String, List<Path>> sstables = new LinkedHashMap<>();
    private final Map<String, Long> nextSequence = new LinkedHashMap<>();

    LsmStorage(Path directory, int memtableEntries) throws IOException {
        if (memtableEntries < 1) {
            throw new IllegalArgumentException("memtableEntries must be positive");
        }
        this.memtableEntries = memtableEntries;
        schemaDirectory = directory.resolve("schema");
        dataDirectory = directory.resolve("data");
        FileDurability.createDirectories(schemaDirectory);
        FileDurability.createDirectories(dataDirectory);
        directoryLock = DirectoryLock.acquire(directory.resolve("toydb.lock"));
        Wal openedWal = null;
        try {
            openedWal = new Wal(directory.resolve("wal.log"));
            FileDurability.forceDirectory(directory);
        } catch (IOException exception) {
            if (openedWal != null) {
                try {
                    openedWal.close();
                } catch (IOException closeFailure) {
                    exception.addSuppressed(closeFailure);
                }
            }
            directoryLock.close();
            throw exception;
        }
        wal = openedWal;
    }

    Map<String, Table> load() throws IOException {
        Map<String, Table> tables = loadCatalog();
        for (Table table : tables.values()) {
            loadSstables(table);
            memtables.put(table.name(), new TreeMap<>());
        }
        wal.recover(batch -> {
            for (Mutation mutation : batch) validateMutation(tables, mutation);
            for (Mutation mutation : batch) {
                Table table = tables.get(mutation.table());
                apply(table, mutation);
                memtables.get(table.name()).put(mutation.key(), mutation);
            }
        });
        return tables;
    }

    void createTable(Table table) throws IOException {
        Path target = schemaDirectory.resolve(table.name() + ".schema");
        Path temporary = schemaDirectory.resolve(table.name() + ".schema.tmp");
        try (FileOutputStream file = new FileOutputStream(temporary.toFile());
             DataOutputStream output = new DataOutputStream(new BufferedOutputStream(file))) {
            output.writeInt(CATALOG_MAGIC);
            output.writeInt(FORMAT_VERSION);
            ValueCodec.writeString(output, table.name());
            output.writeInt(table.primaryKeyIndex());
            output.writeInt(table.columns().size());
            for (Column column : table.columns()) {
                ValueCodec.writeString(output, column.name());
                ValueCodec.writeString(output, column.type().name());
            }
            output.flush();
            file.getChannel().force(true);
        }
        atomicMove(temporary, target);
        FileDurability.forceDirectory(schemaDirectory);
        memtables.put(table.name(), new TreeMap<>());
        sstables.put(table.name(), new ArrayList<>());
        nextSequence.put(table.name(), 1L);
    }

    void commit(List<Mutation> mutations) throws IOException {
        if (mutations.isEmpty()) return;
        wal.append(mutations);
        TreeSet<String> changedTables = new TreeSet<>();
        for (Mutation mutation : mutations) {
            TreeMap<Key, Mutation> memtable = memtables.get(mutation.table());
            if (memtable == null) {
                throw new IOException("storage missing table: " + mutation.table());
            }
            memtable.put(mutation.key(), mutation);
            changedTables.add(mutation.table());
        }
        for (String table : changedTables) {
            if (memtables.get(table).size() >= memtableEntries) {
                try {
                    flush(table);
                } catch (IOException ignored) {
                    // The WAL is already durable. Keep the memtable and retry on close/a later commit.
                }
            }
        }
    }

    private Map<String, Table> loadCatalog() throws IOException {
        Map<String, Table> tables = new LinkedHashMap<>();
        List<Path> schemas;
        try (var paths = Files.list(schemaDirectory)) {
            schemas = paths.filter(path -> path.getFileName().toString().endsWith(".schema"))
                    .sorted().toList();
        }
        for (Path path : schemas) {
            try (DataInputStream input = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(path)))) {
                requireHeader(input, CATALOG_MAGIC, "catalog", path);
                String name = ValueCodec.readString(input);
                int primaryKey = input.readInt();
                int columnCount = input.readInt();
                if (columnCount < 1 || columnCount > 10_000) {
                    throw new IOException("invalid column count in " + path);
                }
                List<Column> columns = new ArrayList<>();
                for (int i = 0; i < columnCount; i++) {
                    columns.add(new Column(ValueCodec.readString(input),
                            ValueType.valueOf(ValueCodec.readString(input))));
                }
                if (primaryKey < 0 || primaryKey >= columns.size()) {
                    throw new IOException("invalid primary key in " + path);
                }
                if (tables.put(name, new Table(name, columns, primaryKey)) != null) {
                    throw new IOException("duplicate table in catalog: " + name);
                }
            } catch (IllegalArgumentException exception) {
                throw new IOException("invalid catalog value in " + path, exception);
            }
        }
        return tables;
    }

    private void loadSstables(Table table) throws IOException {
        Path tableDirectory = dataDirectory.resolve(table.name());
        FileDurability.createDirectories(tableDirectory);
        List<Path> files;
        try (var paths = Files.list(tableDirectory)) {
            files = paths.filter(path -> SSTABLE_NAME.matcher(path.getFileName().toString()).matches())
                    .sorted(Comparator.comparingLong(LsmStorage::sequenceOf)).toList();
        }
        for (Path path : files) {
            readSstable(path, mutation -> apply(table, mutation));
        }
        sstables.put(table.name(), new ArrayList<>(files));
        long next = files.stream().mapToLong(LsmStorage::sequenceOf).max().orElse(0L) + 1;
        nextSequence.put(table.name(), next);
    }

    private void flush(String tableName) throws IOException {
        TreeMap<Key, Mutation> memtable = memtables.get(tableName);
        if (memtable == null || memtable.isEmpty()) return;
        List<Mutation> snapshot = List.copyOf(memtable.values());
        Path path = writeNextSstable(tableName, snapshot);
        for (Mutation mutation : snapshot) {
            memtable.remove(mutation.key(), mutation);
        }
        sstables.get(tableName).add(path);
        if (sstables.get(tableName).size() > COMPACTION_FAN_IN) compact(tableName);
    }

    private void compact(String tableName) throws IOException {
        List<Path> oldFiles = List.copyOf(sstables.get(tableName));
        TreeMap<Key, Mutation> merged = new TreeMap<>();
        for (Path path : oldFiles) {
            readSstable(path, mutation -> merged.put(mutation.key(), mutation));
        }
        Path compacted = writeNextSstable(tableName, new ArrayList<>(merged.values()));
        List<Path> remaining = new ArrayList<>();
        for (Path oldFile : oldFiles) {
            try {
                Files.deleteIfExists(oldFile);
            } catch (IOException exception) {
                remaining.add(oldFile);
            }
        }
        remaining.add(compacted);
        remaining.sort(Comparator.comparingLong(LsmStorage::sequenceOf));
        sstables.put(tableName, remaining);
    }

    private Path writeNextSstable(String tableName, List<Mutation> mutations) throws IOException {
        long sequence = nextSequence.compute(tableName, (ignored, value) -> value == null ? 2 : value + 1) - 1;
        Path tableDirectory = dataDirectory.resolve(tableName);
        FileDurability.createDirectories(tableDirectory);
        Path target = tableDirectory.resolve("sst-%020d.sst".formatted(sequence));
        Path temporary = tableDirectory.resolve(target.getFileName() + ".tmp");
        try (FileOutputStream file = new FileOutputStream(temporary.toFile());
             DataOutputStream output = new DataOutputStream(new BufferedOutputStream(file))) {
            output.writeInt(SSTABLE_MAGIC);
            output.writeInt(FORMAT_VERSION);
            output.writeInt(mutations.size());
            for (Mutation mutation : mutations) MutationCodec.write(output, mutation);
            output.flush();
            file.getChannel().force(true);
        }
        atomicMove(temporary, target);
        FileDurability.forceDirectory(tableDirectory);
        return target;
    }

    private static void readSstable(Path path, Consumer<Mutation> consumer) throws IOException {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            requireHeader(input, SSTABLE_MAGIC, "SSTable", path);
            int count = input.readInt();
            if (count < 0 || count > 100_000_000) {
                throw new IOException("invalid SSTable entry count in " + path);
            }
            for (int i = 0; i < count; i++) consumer.accept(MutationCodec.read(input));
            if (input.read() != -1) throw new IOException("trailing bytes in SSTable " + path);
        }
    }

    private static void requireHeader(DataInputStream input, int magic, String kind, Path path)
            throws IOException {
        if (input.readInt() != magic || input.readInt() != FORMAT_VERSION) {
            throw new IOException("unsupported or corrupt " + kind + " file: " + path);
        }
    }

    private static long sequenceOf(Path path) {
        Matcher matcher = SSTABLE_NAME.matcher(path.getFileName().toString());
        if (!matcher.matches()) throw new IllegalArgumentException("not an SSTable: " + path);
        return Long.parseLong(matcher.group(1));
    }

    private static void apply(Table table, Mutation mutation) {
        if (mutation.row() == null) table.rows().remove(mutation.key());
        else table.rows().put(mutation.key(), mutation.row());
    }

    private static void validateMutation(Map<String, Table> tables, Mutation mutation) throws IOException {
        Table table = tables.get(mutation.table());
        if (table == null) throw new IOException("WAL refers to missing table: " + mutation.table());
        Object keyValue = mutation.key().value();
        if (!table.columns().get(table.primaryKeyIndex()).type().accepts(keyValue)) {
            throw new IOException("WAL primary key type does not match table " + table.name());
        }
        if (mutation.row() == null) return;
        if (mutation.row().size() != table.columns().size()) {
            throw new IOException("WAL row width does not match table " + table.name());
        }
        for (int i = 0; i < mutation.row().size(); i++) {
            if (!table.columns().get(i).type().accepts(mutation.row().get(i))) {
                throw new IOException("WAL value type does not match table " + table.name());
            }
        }
        if (!mutation.key().equals(Key.of(mutation.row().get(table.primaryKeyIndex())))) {
            throw new IOException("WAL row primary key does not match mutation key for table " + table.name());
        }
    }

    private static void atomicMove(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (String table : new ArrayList<>(memtables.keySet())) {
            try {
                flush(table);
            } catch (IOException exception) {
                if (failure == null) failure = exception;
                else failure.addSuppressed(exception);
            }
        }
        try {
            wal.close();
        } catch (IOException exception) {
            if (failure == null) failure = exception;
            else failure.addSuppressed(exception);
        }
        try {
            directoryLock.close();
        } catch (IOException exception) {
            if (failure == null) failure = exception;
            else failure.addSuppressed(exception);
        }
        if (failure != null) throw failure;
    }
}

final class FileDurability {
    private FileDurability() {}

    static void createDirectories(Path directory) throws IOException {
        Path absolute = directory.toAbsolutePath().normalize();
        List<Path> missing = new ArrayList<>();
        for (Path candidate = absolute; candidate != null && Files.notExists(candidate);
             candidate = candidate.getParent()) {
            missing.add(candidate);
        }
        Files.createDirectories(absolute);
        // The list is deepest-first: persist each new entry before its parent entry.
        for (Path created : missing) {
            Path parent = created.getParent();
            if (parent != null) forceDirectory(parent);
        }
    }

    static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}

final class DirectoryLock implements Closeable {
    private final FileChannel channel;
    private final FileLock lock;

    private DirectoryLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    static DirectoryLock acquire(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException exception) {
                lock = null;
            }
            if (lock == null) {
                channel.close();
                throw new IOException("database directory is already open: " + path.getParent());
            }
            return new DirectoryLock(channel, lock);
        } catch (IOException | RuntimeException exception) {
            if (channel.isOpen()) channel.close();
            throw exception;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            lock.release();
        } finally {
            channel.close();
        }
    }
}

record Mutation(String table, Key key, List<Object> row) {
    Mutation {
        if (row != null) row = List.copyOf(row);
    }
}

final class Wal implements Closeable {
    private static final int MAX_RECORD_BYTES = 64 * 1024 * 1024;
    private final RandomAccessFile file;

    Wal(Path path) throws IOException {
        file = new RandomAccessFile(path.toFile(), "rw");
    }

    synchronized void append(List<Mutation> mutations) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(mutations.size());
            for (Mutation mutation : mutations) MutationCodec.write(output, mutation);
        }
        byte[] payload = bytes.toByteArray();
        if (payload.length > MAX_RECORD_BYTES) {
            throw new IOException("transaction exceeds the 64 MiB WAL record limit");
        }
        CRC32 crc = new CRC32();
        crc.update(payload);
        file.seek(file.length());
        file.writeInt(payload.length);
        file.writeInt(~payload.length);
        file.writeInt((int) crc.getValue());
        file.write(payload);
        file.getFD().sync();
    }

    synchronized void recover(WalBatchConsumer consumer) throws IOException {
        file.seek(0);
        while (file.getFilePointer() < file.length()) {
            long recordStart = file.getFilePointer();
            if (file.length() - recordStart < Integer.BYTES * 3L) {
                file.setLength(recordStart);
                break;
            }
            int length = file.readInt();
            int complementedLength = file.readInt();
            int expectedCrc = file.readInt();
            if (complementedLength != ~length) {
                throw new IOException("WAL length checksum mismatch at offset " + recordStart);
            }
            if (length < 0 || length > MAX_RECORD_BYTES) {
                throw new IOException("invalid WAL record length at offset " + recordStart);
            }
            if (file.length() - file.getFilePointer() < length) {
                file.setLength(recordStart);
                break;
            }
            byte[] payload = new byte[length];
            file.readFully(payload);
            CRC32 crc = new CRC32();
            crc.update(payload);
            if ((int) crc.getValue() != expectedCrc) {
                throw new IOException("WAL checksum mismatch at offset " + recordStart);
            }
            List<Mutation> batch;
            try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
                int count = input.readInt();
                if (count < 0 || count > 10_000_000) {
                    throw new IOException("invalid WAL mutation count at offset " + recordStart);
                }
                batch = new ArrayList<>(count);
                for (int i = 0; i < count; i++) batch.add(MutationCodec.read(input));
                if (input.read() != -1) {
                    throw new IOException("trailing bytes in WAL record at offset " + recordStart);
                }
            } catch (EOFException exception) {
                throw new IOException("malformed WAL record at offset " + recordStart, exception);
            }
            consumer.accept(List.copyOf(batch));
        }
        file.seek(file.length());
    }

    @Override
    public synchronized void close() throws IOException {
        file.getFD().sync();
        file.close();
    }
}

@FunctionalInterface
interface WalBatchConsumer {
    void accept(List<Mutation> batch) throws IOException;
}

final class MutationCodec {
    private MutationCodec() {}

    static void write(DataOutputStream output, Mutation mutation) throws IOException {
        ValueCodec.writeString(output, mutation.table());
        ValueCodec.write(output, mutation.key().value());
        output.writeBoolean(mutation.row() == null);
        if (mutation.row() != null) {
            output.writeInt(mutation.row().size());
            for (Object value : mutation.row()) ValueCodec.write(output, value);
        }
    }

    static Mutation read(DataInputStream input) throws IOException {
        String table = ValueCodec.readString(input);
        Key key = Key.of(ValueCodec.read(input));
        if (input.readBoolean()) return new Mutation(table, key, null);
        int values = input.readInt();
        if (values < 0 || values > 10_000) throw new IOException("invalid row size");
        List<Object> row = new ArrayList<>(values);
        for (int i = 0; i < values; i++) row.add(ValueCodec.read(input));
        return new Mutation(table, key, row);
    }
}

final class ValueCodec {
    private static final int LONG = 1;
    private static final int STRING = 2;
    private static final int BOOLEAN = 3;
    static final int MAX_STRING_BYTES = 16 * 1024 * 1024;

    private ValueCodec() {}

    static void write(DataOutputStream output, Object value) throws IOException {
        if (value instanceof Long number) {
            output.writeByte(LONG);
            output.writeLong(number);
        } else if (value instanceof String text) {
            output.writeByte(STRING);
            writeString(output, text);
        } else if (value instanceof Boolean bool) {
            output.writeByte(BOOLEAN);
            output.writeBoolean(bool);
        } else {
            throw new IOException("unsupported stored value: " + value);
        }
    }

    static Object read(DataInputStream input) throws IOException {
        return switch (input.readUnsignedByte()) {
            case LONG -> input.readLong();
            case STRING -> readString(input);
            case BOOLEAN -> input.readBoolean();
            default -> throw new IOException("invalid stored value type");
        };
    }

    static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) throw new IOException("string exceeds the 16 MiB limit");
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    static String readString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > MAX_STRING_BYTES) throw new IOException("invalid string length");
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
