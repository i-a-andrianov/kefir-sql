package org.kefir.sql

public interface Connection : Closeable {
    public fun query(queryString: String, vararg params: Any): QueryResult
}
