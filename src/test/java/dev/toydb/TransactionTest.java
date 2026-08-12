package dev.toydb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TransactionTest {
    @TempDir
    Path directory;

    @Test
    void commitPublishesAndRollbackRestoresAllChanges() throws Exception {
        try (Database database = Database.open(directory);
             Session session = database.connect()) {
            session.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)");
            session.execute("INSERT INTO accounts VALUES (1, 100)");

            session.execute("BEGIN");
            session.execute("UPDATE accounts SET balance = 75 WHERE id = 1");
            session.execute("INSERT INTO accounts VALUES (2, 25)");
            assertEquals(List.of(List.of(1L, 75L), List.of(2L, 25L)),
                    session.execute("SELECT * FROM accounts").rows());
            session.execute("ROLLBACK");
            assertEquals(List.of(List.of(1L, 100L)),
                    session.execute("SELECT * FROM accounts").rows());

            session.execute("BEGIN TRANSACTION");
            session.execute("UPDATE accounts SET balance = 80 WHERE id = 1");
            session.execute("COMMIT");
            assertEquals(List.of(List.of(1L, 80L)),
                    session.execute("SELECT * FROM accounts").rows());
        }
    }

    @Test
    void concurrentSerializableTransactionsRunInACompleteOrder() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (Database database = Database.open(directory);
             Session first = database.connect();
             Session second = database.connect()) {
            first.execute("CREATE TABLE counters (id INT PRIMARY KEY, value INT)");
            first.execute("INSERT INTO counters VALUES (1, 10)");
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch secondAttempting = new CountDownLatch(1);

            Future<?> firstTransaction = executor.submit(() -> {
                first.execute("BEGIN");
                firstStarted.countDown();
                await(secondAttempting);
                first.execute("UPDATE counters SET value = 11 WHERE id = 1");
                first.execute("COMMIT");
            });
            Future<Long> secondTransaction = executor.submit(() -> {
                await(firstStarted);
                secondAttempting.countDown();
                second.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                long observed = (Long) second.execute("SELECT * FROM counters").rows().get(0).get(1);
                second.execute("UPDATE counters SET value = 12 WHERE id = 1");
                second.execute("COMMIT");
                return observed;
            });

            firstTransaction.get(5, TimeUnit.SECONDS);
            assertEquals(11L, secondTransaction.get(5, TimeUnit.SECONDS));
            assertEquals(12L, first.execute("SELECT * FROM counters").rows().get(0).get(1));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void databaseCloseDoesNotDeadlockBehindAQueuedSession() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Database database = Database.open(directory);
        Session owner = database.connect();
        Session queued = database.connect();
        try {
            owner.execute("CREATE TABLE counters (id INT PRIMARY KEY, value INT)");
            owner.execute("INSERT INTO counters VALUES (1, 10)");
            owner.execute("BEGIN");
            owner.execute("UPDATE counters SET value = 99 WHERE id = 1");
            CountDownLatch queuedStatementStarted = new CountDownLatch(1);
            Future<Long> queuedRead = executor.submit(() -> {
                queuedStatementStarted.countDown();
                return (Long) queued.execute("SELECT * FROM counters").rows().get(0).get(1);
            });
            await(queuedStatementStarted);
            awaitExecuting(queued);

            Future<?> close = executor.submit(() -> {
                try {
                    database.close();
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            });

            close.get(5, TimeUnit.SECONDS);
            assertEquals(10L, queuedRead.get(5, TimeUnit.SECONDS));
            try (Database reopened = Database.open(directory);
                 Session session = reopened.connect()) {
                assertEquals(10L, session.execute("SELECT * FROM counters").rows().get(0).get(1));
            }
        } finally {
            database.close();
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for transaction condition");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for transaction condition", exception);
        }
    }

    private static void awaitExecuting(Session session) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!session.isExecuting()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("timed out waiting for queued session to start executing");
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for queued session", exception);
            }
        }
    }
}
