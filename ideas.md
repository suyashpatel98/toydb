# Consumed
 Currently it doesn't support syntax like INSERT ... ON CONFLICT. Add support for that

 Currently it doesn't support the following syntax - ALTER TABLE. Add support for that

 Currently it doesn't support the following syntax - CREATE INDEX. Add support for that

 currently, the server listens on raw TCP and uses bincode framing. Add support for TLS encryption.



 Add support for MVCC garbage collection and abandoned-transaction recovery (note that explicit rollback exists, but automatic recovery of abandoned transactions does not)


 currently, query cancellation and statement timeouts aren't supported. Add support for that. Allow clients to stop expensive or blocked queries


implement views and materialized views

 Currently it doesn't support set operations like UNION, INTERSECT, and EXCEPT. You need to add support for that


 Currently it only supports Boolean, integer, float, and string. Add support for UUID and JSON data types natively.

 Currently, it doesn't support prepared statements and binding parameters — add support for that. So you should parse once, execute repeatedly, and avoid interpolating user values into SQL.


 Currently, it doesn't support named checkpoints. Add support for syntax like SAVEPOINT, ROLLBACK TO SAVEPOINT, and RELEASE SAVEPOINT

 Currently, it doesn't have support for subqueries, EXISTS, and IN (SELECT ...) features. Add support for that


 Currently, there is no record of statements that exceed latency or resource thresholds. Add configurable slow-query logs - you can decide what metrics to include in the logs

# New ideas

 Currently it only supports `SELECT *`. Add explicit projections, column aliases, literals, and simple computed expressions in `SELECT` lists.

 Currently result order is only the table's primary-key iteration order. Add `ORDER BY`, plus `LIMIT` and `OFFSET` for paginated reads.

 Currently predicates only support comparisons joined by `AND`. Add richer boolean expressions with `OR`, `NOT`, parentheses, `BETWEEN`, and `LIKE`.

 Currently it does not support joins. Add `INNER JOIN` and `LEFT JOIN` across tables, including qualified column names like `users.id`.

 Currently it does not support aggregate queries. Add `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`, with `GROUP BY` and `HAVING`.

 Currently every column must be supplied on insert and there is no SQL `NULL`. Add `NULL` values, `IS NULL`/`IS NOT NULL`, column defaults, and `NOT NULL` constraints.

 Currently only primary-key uniqueness is enforced. Add table constraints such as `UNIQUE`, `CHECK`, and foreign keys with configurable `ON DELETE` behavior.

 Currently tables can be created but not removed. Add `DROP TABLE` and `TRUNCATE TABLE`, with durable catalog and data-file cleanup.

 Currently there are no catalog introspection statements. Add `SHOW TABLES`, `DESCRIBE table`, and an `information_schema`-style metadata view.

 Currently there is no query-plan visibility. Add `EXPLAIN` and `EXPLAIN ANALYZE` so users can see scans, filters, row counts, and timing.

 Currently data must be inserted row by row through SQL. Add bulk import/export support, such as `COPY table FROM/TO` CSV files.

 Currently the storage engine appends to the WAL and compacts SSTables, but exposes no maintenance commands. Add manual `CHECKPOINT`/`VACUUM` commands to truncate replay history and reclaim tombstoned data.

 Currently backups require copying the database directory externally. Add online backup and restore APIs that produce a consistent snapshot while sessions are active.

 Currently unsupported SQL fails with a generic parser error. Add structured error codes, SQLSTATE-like categories, and precise parse positions.

 Currently the shell prints tab-separated output only. Add shell commands for `.tables`, `.schema`, `.mode`, `.headers`, and aligned table output.

 Currently values are limited to integers, text, and booleans. Add `DATE`, `TIME`, `TIMESTAMP`, `DECIMAL`, and binary `BLOB` types.
