package dev.toydb;

import java.util.List;

/** Result of a SQL statement. Queries have rows; mutations have an update count. */
public record SqlResult(List<String> columns, List<List<Object>> rows, int updateCount) {
    public SqlResult {
        columns = List.copyOf(columns);
        rows = rows.stream().map(List::copyOf).toList();
    }

    static SqlResult query(List<String> columns, List<List<Object>> rows) {
        return new SqlResult(columns, rows, -1);
    }

    static SqlResult update(int count) {
        return new SqlResult(List.of(), List.of(), count);
    }
}
