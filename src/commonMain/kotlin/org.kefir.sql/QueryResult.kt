package org.kefir.sql

public interface QueryResult : Iterable<QueryResultRow>, Closeable {
    public fun rowsAffected(): Int?
}
