package dev.toydb;

import java.util.Objects;
import java.util.function.Consumer;

/** A logical database connection. Sessions are not thread-safe. */
public final class Session implements AutoCloseable {
    private final Engine engine;
    private final Consumer<Session> onClose;
    private Transaction transaction;
    private boolean closeRequested;
    private boolean executing;
    private boolean released;

    Session(Engine engine, Consumer<Session> onClose) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.onClose = Objects.requireNonNull(onClose, "onClose");
    }

    public SqlResult execute(String sql) {
        synchronized (this) {
            if (closeRequested) throw new DbException("session is closed");
            if (executing) throw new DbException("concurrent use of one session is not supported");
            executing = true;
        }
        try {
            return executeInternal(sql);
        } finally {
            finishExecution();
        }
    }

    private SqlResult executeInternal(String sql) {
        String command = transactionCommand(sql);
        return switch (command) {
            case "BEGIN", "BEGIN TRANSACTION", "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE" -> begin();
            case "COMMIT", "COMMIT TRANSACTION" -> commit();
            case "ROLLBACK", "ROLLBACK TRANSACTION" -> rollback();
            default -> transaction == null ? engine.execute(sql) : engine.execute(transaction, sql);
        };
    }

    private SqlResult begin() {
        if (transaction != null) {
            throw new DbException("a transaction is already active");
        }
        transaction = engine.begin();
        return SqlResult.update(0);
    }

    private SqlResult commit() {
        requireTransaction();
        Transaction committing = transaction;
        transaction = null;
        engine.commit(committing);
        return SqlResult.update(0);
    }

    private SqlResult rollback() {
        requireTransaction();
        engine.rollback(transaction);
        transaction = null;
        return SqlResult.update(0);
    }

    private void requireTransaction() {
        if (transaction == null) {
            throw new DbException("no transaction is active");
        }
    }

    private static String transactionCommand(String sql) {
        if (sql == null) return "";
        String command = sql.trim();
        if (command.endsWith(";")) command = command.substring(0, command.length() - 1).trim();
        return command.replaceAll("\\s+", " ").toUpperCase(java.util.Locale.ROOT);
    }

    void requestClose() {
        Transaction rollback = null;
        boolean canRelease;
        synchronized (this) {
            if (closeRequested) return;
            closeRequested = true;
            if (!executing && transaction != null) {
                rollback = transaction;
                transaction = null;
            }
            canRelease = !executing;
        }
        if (rollback != null) {
            try {
                engine.rollback(rollback);
            } finally {
                completeClose();
            }
        } else if (canRelease) {
            completeClose();
        }
    }

    void awaitClosed() {
        boolean interrupted = false;
        synchronized (this) {
            while (!released) {
                try {
                    wait();
                } catch (InterruptedException exception) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    synchronized boolean isExecuting() {
        return executing;
    }

    private void finishExecution() {
        Transaction rollback = null;
        boolean shouldClose;
        synchronized (this) {
            executing = false;
            shouldClose = closeRequested;
            if (shouldClose && transaction != null) {
                rollback = transaction;
                transaction = null;
            }
        }
        if (rollback != null) {
            try {
                engine.rollback(rollback);
            } finally {
                completeClose();
            }
        } else if (shouldClose) {
            completeClose();
        }
    }

    private void completeClose() {
        boolean notifyDatabase = false;
        synchronized (this) {
            if (!released && closeRequested && !executing && transaction == null) {
                released = true;
                notifyAll();
                notifyDatabase = true;
            }
        }
        if (notifyDatabase) onClose.accept(this);
    }

    @Override
    public void close() {
        requestClose();
        awaitClosed();
    }
}
