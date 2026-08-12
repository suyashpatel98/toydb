package dev.toydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CrudTest {
    @TempDir
    Path directory;

    @Test
    void createsTableInsertsRowsAndSelectsAllColumns() throws Exception {
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            session.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOLEAN)");

            assertEquals(1, session.execute(
                    "INSERT INTO users (id, name, active) VALUES (1, 'Ada', true)").updateCount());
            assertEquals(1, session.execute(
                    "INSERT INTO users VALUES (2, 'Linus', false)").updateCount());

            SqlResult result = session.execute("SELECT * FROM users");
            assertEquals(List.of("id", "name", "active"), result.columns());
            assertEquals(List.of(
                    List.of(1L, "Ada", true),
                    List.of(2L, "Linus", false)), result.rows());
        }
    }

    @Test
    void updatesMatchingRowsAndSupportsSelectPredicates() throws Exception {
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            session.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOLEAN)");
            session.execute("INSERT INTO users VALUES (1, 'Ada', true)");
            session.execute("INSERT INTO users VALUES (2, 'Linus', true)");

            assertEquals(1, session.execute(
                    "UPDATE users SET name = 'Grace', active = false WHERE id = 1").updateCount());
            assertEquals(List.of(List.of(1L, "Grace", false)),
                    session.execute("SELECT * FROM users WHERE active = false").rows());
        }
    }

    @Test
    void deletesOnlyMatchingRows() throws Exception {
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            session.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOLEAN)");
            session.execute("INSERT INTO users VALUES (1, 'Ada', true)");
            session.execute("INSERT INTO users VALUES (2, 'Linus', false)");
            session.execute("INSERT INTO users VALUES (3, 'Grace', false)");

            assertEquals(2, session.execute("DELETE FROM users WHERE active = false").updateCount());
            assertEquals(List.of(List.of(1L, "Ada", true)),
                    session.execute("SELECT * FROM users").rows());
        }
    }

    @Test
    void preservesSqlPunctuationInsideTextValues() throws Exception {
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            session.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOLEAN)");
            session.execute("INSERT INTO users VALUES (1, 'Ada, O''Brien', true)");

            session.execute("UPDATE users SET name = 'Where, exactly WHERE now' WHERE id = 1");

            assertEquals(List.of(List.of(1L, "Where, exactly WHERE now", true)),
                    session.execute("SELECT * FROM users WHERE name = 'Where, exactly WHERE now'").rows());
        }
    }

    @Test
    void rejectsColumnNamesThatSqlCannotReference() throws Exception {
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            assertThrows(DbException.class,
                    () -> session.execute("CREATE TABLE broken (bad-name INT PRIMARY KEY)"));
        }
    }
}
