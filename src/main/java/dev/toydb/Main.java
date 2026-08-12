package dev.toydb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Command-line SQL shell. Pass the database directory as the sole argument. */
public final class Main {
    private Main() {}

    public static void main(String[] args) {
        System.exit(run(args, System.in, System.out, System.err));
    }

    static int run(String[] args, InputStream input, PrintStream output, PrintStream errors) {
        if (args.length > 1) {
            errors.println("usage: java -jar toydb.jar [database-directory]");
            return 2;
        }
        Path directory = Path.of(args.length == 0 ? "toydb-data" : args[0]);
        boolean interactive = input == System.in && System.console() != null;
        int status = 0;
        try (Database database = Database.open(directory);
             Session session = database.connect();
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(input, StandardCharsets.UTF_8))) {
            StringBuilder pending = new StringBuilder();
            while (true) {
                if (interactive) {
                    output.print(pending.length() == 0 ? "toydb> " : "   ...> ");
                    output.flush();
                }
                String line = reader.readLine();
                if (line == null) break;
                if (pending.length() == 0
                        && (line.trim().equalsIgnoreCase("quit") || line.trim().equalsIgnoreCase("exit"))) {
                    break;
                }
                pending.append(line).append('\n');
                for (String statement : completeStatements(pending)) {
                    try {
                        print(session.execute(statement), output);
                    } catch (DbException exception) {
                        errors.println("error: " + exception.getMessage());
                        status = 1;
                    }
                }
            }
            if (!pending.toString().isBlank()) {
                try {
                    print(session.execute(pending.toString()), output);
                } catch (DbException exception) {
                    errors.println("error: " + exception.getMessage());
                    status = 1;
                }
            }
        } catch (IOException exception) {
            errors.println("error: " + exception.getMessage());
            return 1;
        }
        return status;
    }

    private static List<String> completeStatements(StringBuilder pending) {
        List<String> statements = new ArrayList<>();
        boolean quoted = false;
        int statementStart = 0;
        int consumed = 0;
        for (int i = 0; i < pending.length(); i++) {
            char c = pending.charAt(i);
            if (c == '\'' && quoted && i + 1 < pending.length() && pending.charAt(i + 1) == '\'') {
                i++;
            } else if (c == '\'') {
                quoted = !quoted;
            } else if (c == ';' && !quoted) {
                String statement = pending.substring(statementStart, i).trim();
                if (!statement.isEmpty()) statements.add(statement);
                statementStart = i + 1;
                consumed = statementStart;
            }
        }
        if (consumed > 0) pending.delete(0, consumed);
        return statements;
    }

    private static void print(SqlResult result, PrintStream output) {
        if (result.updateCount() >= 0) {
            output.println("OK (" + result.updateCount() + " rows)");
            return;
        }
        output.println(String.join("\t", result.columns()));
        for (List<Object> row : result.rows()) {
            output.println(row.stream().map(String::valueOf).reduce((left, right) -> left + "\t" + right)
                    .orElse(""));
        }
        output.println("(" + result.rows().size() + " rows)");
    }
}
