package dev.toydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MainTest {
    @TempDir
    Path directory;

    @Test
    void runsSqlScriptsFromStandardInput() {
        String script = """
                CREATE TABLE messages (id INT PRIMARY KEY, body TEXT);
                INSERT INTO messages VALUES (1, 'hello; world');
                SELECT * FROM messages;
                """;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream errors = new ByteArrayOutputStream();

        int status = Main.run(new String[] {directory.toString()},
                new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8)),
                new PrintStream(output), new PrintStream(errors));

        assertEquals(0, status);
        assertEquals("", errors.toString(StandardCharsets.UTF_8));
        String printed = output.toString(StandardCharsets.UTF_8);
        assertTrue(printed.contains("id\tbody"));
        assertTrue(printed.contains("1\thello; world"));
    }
}
