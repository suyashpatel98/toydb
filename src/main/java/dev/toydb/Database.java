package dev.toydb;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Entry point for an embedded ToyDB instance. */
public final class Database implements AutoCloseable {
    private final Engine engine;
    private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private boolean closed;

    private Database(Path directory, int memtableEntries) throws IOException {
        Path durableDirectory = directory.toAbsolutePath().normalize();
        FileDurability.createDirectories(durableDirectory);
        this.engine = new Engine(durableDirectory, memtableEntries);
    }

    public static Database open(Path directory) throws IOException {
        return open(directory, LsmStorage.DEFAULT_MEMTABLE_ENTRIES);
    }

    /** Opens a database with a custom memtable flush threshold, useful for small deployments/tests. */
    public static Database open(Path directory, int memtableEntries) throws IOException {
        return new Database(Objects.requireNonNull(directory, "directory"), memtableEntries);
    }

    public synchronized Session connect() {
        if (closed) {
            throw new DbException("database is closed");
        }
        Session session = new Session(engine, sessions::remove);
        sessions.add(session);
        return session;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            List<Session> openSessions = List.copyOf(sessions);
            // Signal every session first so a gate owner can roll back before we await queued work.
            for (Session session : openSessions) session.requestClose();
            for (Session session : openSessions) session.awaitClosed();
            engine.close();
        }
    }
}
