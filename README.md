# ToyDB

ToyDB is a deliberately small, embedded, single-node SQL database written in Java 17. It supports the requested CRUD surface, durable serializable transactions, and a compact LSM-style storage engine without third-party runtime dependencies.

## Build and run

```bash
mvn test
mvn package
java -jar target/toydb-1.0-SNAPSHOT.jar ./my-database
```

The shell accepts semicolon-terminated statements from a terminal or stdin. Type `exit` or `quit` to leave an interactive shell.

```sql
CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT, balance INT, active BOOLEAN);
INSERT INTO accounts VALUES (1, 'Ada', 100, true);
INSERT INTO accounts (id, owner, balance, active) VALUES (2, 'Grace', 50, true);
SELECT * FROM accounts WHERE active = true AND balance >= 50;
UPDATE accounts SET balance = 75 WHERE id = 2;
DELETE FROM accounts WHERE active = false;

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
UPDATE accounts SET balance = 90 WHERE id = 1;
UPDATE accounts SET balance = 60 WHERE id = 2;
COMMIT;
```

`BEGIN` and `BEGIN TRANSACTION` are equivalent; serializable is always the isolation level. `ROLLBACK` discards all changes made since `BEGIN`. Closing a session with an open transaction also rolls it back.

## Java API

```java
try (Database db = Database.open(Path.of("./my-database"));
     Session session = db.connect()) {
    session.execute("CREATE TABLE messages (id INT PRIMARY KEY, body TEXT)");
    session.execute("INSERT INTO messages VALUES (1, 'hello')");
    SqlResult result = session.execute("SELECT * FROM messages");
    System.out.println(result.rows());
}
```

One `Database` can create multiple sessions. A directory is protected by an OS file lock and cannot be opened by a second `Database` instance at the same time.

## SQL scope

- `CREATE TABLE` with exactly one inline `PRIMARY KEY`
- Types: `INT` (returned as Java `Long`), `TEXT`, and `BOOLEAN`
- `INSERT INTO ... VALUES ...`, with an optional complete column list
- `SELECT * FROM ...` with an optional `WHERE`
- `UPDATE ... SET ...` and `DELETE FROM ...`, with an optional `WHERE`
- `WHERE` supports `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, joined by `AND`
- Text uses single quotes and escapes a quote as `''`
- `BEGIN`, `COMMIT`, and `ROLLBACK`

This intentionally omits joins, projections other than `*`, indexes, `NULL`, schema changes, authentication, and a network protocol. `CREATE TABLE` is an autocommit operation and is rejected inside an explicit transaction.

`TEXT` values are limited to 16 MiB, and one committed transaction may occupy at most 64 MiB in the WAL. Closing a `Database` closes its sessions and rolls back any transaction they still own.

## Storage and isolation

Committed mutations are first written as one checksummed batch to `wal.log` and fsynced. They then enter an ordered memtable. At the flush threshold, the memtable becomes an immutable, sorted SSTable under `data/<table>/`; more than four SSTables are compacted. Catalog schemas are stored under `schema/`. File contents and their parent-directory entries are synced before durability is acknowledged. Recovery loads SSTables and replays only complete, checksummed WAL batches; an incomplete final record is discarded, while a checksum or logical corruption refuses to open the database without deleting evidence.

Serializable isolation uses strict two-phase locking at database granularity. An explicit transaction owns a fair global transaction gate from `BEGIN` through `COMMIT` or `ROLLBACK`; autocommit statements acquire the same gate for one statement. This sacrifices concurrent throughput but prevents dirty reads, non-repeatable reads, phantoms, and write skew with a small, auditable implementation.
