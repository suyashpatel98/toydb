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
