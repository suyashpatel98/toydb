package dev.toydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.io.RandomAccessFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageTest {
    @TempDir
    Path directory;

    @Test
    void committedRowsSurviveReopenAndRolledBackRowsDoNot() throws Exception {
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            session.execute("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)");
            session.execute("INSERT INTO notes VALUES (1, 'durable')");
            session.execute("BEGIN");
            session.execute("INSERT INTO notes VALUES (2, 'committed transaction')");
            session.execute("COMMIT");
            session.execute("BEGIN");
            session.execute("DELETE FROM notes WHERE id = 1");
            session.execute("INSERT INTO notes VALUES (3, 'rolled back')");
            session.execute("ROLLBACK");
        }

        try (Database reopened = Database.open(directory);
             Session session = reopened.connect()) {
            assertEquals(List.of(
                    List.of(1L, "durable"),
                    List.of(2L, "committed transaction")),
                    session.execute("SELECT * FROM notes").rows());
        }
    }

    @Test
    void flushesSortedTablesAndCompactsThemWithoutLosingRows() throws Exception {
        try (Database database = Database.open(directory, 1);
             Session session = database.connect()) {
            session.execute("CREATE TABLE items (id INT PRIMARY KEY, label TEXT)");
            for (int i = 1; i <= 7; i++) {
                session.execute("INSERT INTO items VALUES (" + i + ", 'item " + i + "')");
            }
        }

        Path tableDirectory = directory.resolve("data/items");
        long sortedTables;
        try (var files = Files.list(tableDirectory)) {
            sortedTables = files.filter(path -> path.getFileName().toString().endsWith(".sst")).count();
        }
        assertTrue(sortedTables >= 1 && sortedTables <= 4,
                "compaction should leave a small set of immutable SSTables");

        try (Database reopened = Database.open(directory, 1);
             Session session = reopened.connect()) {
            assertEquals(7, session.execute("SELECT * FROM items").rows().size());
            assertEquals(List.of(7L, "item 7"),
                    session.execute("SELECT * FROM items WHERE id = 7").rows().get(0));
        }
    }

    @Test
    void preventsTwoDatabaseInstancesFromWritingTheSameDirectory() throws Exception {
        try (Database ignored = Database.open(directory)) {
            assertThrows(java.io.IOException.class, () -> Database.open(directory));
        }
    }

    @Test
    void recoversOnlyCompleteCommittedWalBatchesAfterACrash() throws Exception {
        Path live = directory.resolve("live");
        Path crashImage = directory.resolve("crash-image");
        try (Database database = Database.open(live);
             Session session = database.connect()) {
            session.execute("CREATE TABLE events (id INT PRIMARY KEY, label TEXT)");
            session.execute("INSERT INTO events VALUES (1, 'autocommit')");
            session.execute("BEGIN");
            session.execute("INSERT INTO events VALUES (2, 'transaction')");
            session.execute("COMMIT");
            session.execute("BEGIN");
            session.execute("INSERT INTO events VALUES (3, 'rollback')");
            session.execute("ROLLBACK");

            Files.createDirectories(crashImage.resolve("schema"));
            Files.copy(live.resolve("schema/events.schema"),
                    crashImage.resolve("schema/events.schema"));
            Files.copy(live.resolve("wal.log"), crashImage.resolve("wal.log"));
        }

        try (Database recovered = Database.open(crashImage);
             Session session = recovered.connect()) {
            assertEquals(List.of(
                    List.of(1L, "autocommit"),
                    List.of(2L, "transaction")),
                    session.execute("SELECT * FROM events").rows());
        }
    }

    @Test
    void refusesMidLogCorruptionWithoutDeletingWalHistory() throws Exception {
        Path walPath = directory.resolve("wal.log");
        try (Wal wal = new Wal(walPath)) {
            wal.append(List.of(new Mutation("items", Key.of(1L), List.of(1L, "one"))));
            wal.append(List.of(new Mutation("items", Key.of(2L), List.of(2L, "two"))));
        }
        long originalLength = Files.size(walPath);
        try (RandomAccessFile file = new RandomAccessFile(walPath.toFile(), "rw")) {
            long payloadOffset = Integer.BYTES * 3L;
            file.seek(payloadOffset);
            int original = file.readUnsignedByte();
            file.seek(payloadOffset);
            file.writeByte(original ^ 0xff);
        }

        try (Wal corrupted = new Wal(walPath)) {
            assertThrows(java.io.IOException.class, () -> corrupted.recover(batch -> {}));
        }
        assertEquals(originalLength, Files.size(walPath),
                "mid-log corruption must not be mistaken for an incomplete tail");
    }

    @Test
    void refusesCorruptedWalLengthWithoutTreatingItAsATornTail() throws Exception {
        Path walPath = directory.resolve("wal.log");
        try (Wal wal = new Wal(walPath)) {
            wal.append(List.of(new Mutation("items", Key.of(1L), List.of(1L, "one"))));
            wal.append(List.of(new Mutation("items", Key.of(2L), List.of(2L, "two"))));
        }
        long originalLength = Files.size(walPath);
        try (RandomAccessFile file = new RandomAccessFile(walPath.toFile(), "rw")) {
            file.seek(0);
            file.writeInt((int) originalLength);
        }

        try (Wal corrupted = new Wal(walPath)) {
            assertThrows(java.io.IOException.class, () -> corrupted.recover(batch -> {}));
        }
        assertEquals(originalLength, Files.size(walPath));
    }

    @Test
    void rejectsTextThatCannotBeReadBackBeforeCommittingIt() throws Exception {
        String oversized = "x".repeat(16 * 1024 * 1024 + 1);
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            session.execute("CREATE TABLE documents (id INT PRIMARY KEY, body TEXT)");
            assertThrows(DbException.class,
                    () -> session.execute("INSERT INTO documents VALUES (1, '" + oversized + "')"));
            assertEquals(List.of(), session.execute("SELECT * FROM documents").rows());
        }
        try (Database reopened = Database.open(directory);
             Session session = reopened.connect()) {
            assertEquals(List.of(), session.execute("SELECT * FROM documents").rows());
        }
    }

    @Test
    void closingDatabaseRollsBackAndInvalidatesItsSessions() throws Exception {
        Database database = Database.open(directory);
        Session session = database.connect();
        session.execute("CREATE TABLE tasks (id INT PRIMARY KEY, label TEXT)");
        session.execute("BEGIN");
        session.execute("INSERT INTO tasks VALUES (1, 'uncommitted')");

        database.close();

        assertThrows(DbException.class, () -> session.execute("SELECT * FROM tasks"));
        try (Database reopened = Database.open(directory);
             Session reopenedSession = reopened.connect()) {
            assertEquals(List.of(), reopenedSession.execute("SELECT * FROM tasks").rows());
        }
    }

    @Test
    void truncatesOnlyAnIncompleteFinalWalRecord() throws Exception {
        Path walPath = directory.resolve("wal.log");
        long firstRecordLength;
        try (Wal wal = new Wal(walPath)) {
            wal.append(List.of(new Mutation("items", Key.of(1L), List.of(1L, "one"))));
            firstRecordLength = Files.size(walPath);
            wal.append(List.of(new Mutation("items", Key.of(2L), List.of(2L, "two"))));
        }
        try (RandomAccessFile file = new RandomAccessFile(walPath.toFile(), "rw")) {
            file.setLength(file.length() - 3);
        }
        List<Mutation> recovered = new ArrayList<>();
        try (Wal wal = new Wal(walPath)) {
            wal.recover(recovered::addAll);
        }
        assertEquals(List.of(new Mutation("items", Key.of(1L), List.of(1L, "one"))), recovered);
        assertEquals(firstRecordLength, Files.size(walPath));
    }

    @Test
    void rejectsAnInvalidMultiMutationWalBatchAtomically() throws Exception {
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            session.execute("CREATE TABLE items (id INT PRIMARY KEY, label TEXT)");
        }
        try (Wal wal = new Wal(directory.resolve("wal.log"))) {
            wal.append(List.of(
                    new Mutation("items", Key.of(1L), List.of(1L, "valid first mutation")),
                    new Mutation("missing_table", Key.of(2L), List.of(2L, "invalid second mutation"))));
        }
        assertThrows(java.io.IOException.class, () -> Database.open(directory));
    }

    @Test
    void sstablesReconstructUpdatesAndTombstonesWithoutTheWal() throws Exception {
        Path source = directory.resolve("source");
        Path dataOnly = directory.resolve("data-only");
        try (Database database = Database.open(source, 1);
             Session session = database.connect()) {
            session.execute("CREATE TABLE items (id INT PRIMARY KEY, label TEXT)");
            session.execute("INSERT INTO items VALUES (1, 'old')");
            session.execute("INSERT INTO items VALUES (2, 'deleted')");
            session.execute("UPDATE items SET label = 'new' WHERE id = 1");
            session.execute("DELETE FROM items WHERE id = 2");
        }
        Files.createDirectories(dataOnly.resolve("schema"));
        Files.createDirectories(dataOnly.resolve("data/items"));
        Files.copy(source.resolve("schema/items.schema"), dataOnly.resolve("schema/items.schema"));
        try (var files = Files.list(source.resolve("data/items"))) {
            for (Path file : files.filter(path -> path.getFileName().toString().endsWith(".sst")).toList()) {
                Files.copy(file, dataOnly.resolve("data/items").resolve(file.getFileName()));
            }
        }

        try (Database reopened = Database.open(dataOnly);
             Session session = reopened.connect()) {
            assertEquals(List.of(List.of(1L, "new")), session.execute("SELECT * FROM items").rows());
        }
    }
}
