package org.kefir.sql.jdbc

import org.kefir.sql.QueryResult
import org.kefir.sql.QueryResultRow
import java.sql.ResultSet

internal class JdbcQueryResult(
    private val resultSet: ResultSet?,
    private val updateCount: Int?
) : QueryResult {
    override fun rowsAffected(): Int? = updateCount

    override fun iterator(): Iterator<QueryResultRow> {
        if (resultSet == null) return emptyList<QueryResultRow>().iterator()
        return ResultIterator()
    }

    private inner class ResultIterator : Iterator<QueryResultRow> {
        private var hasMoreRows = resultSet!!.next()

        override fun hasNext(): Boolean = hasMoreRows

        override fun next(): QueryResultRow {
            if (!hasNext()) throw NoSuchElementException("no more elements")

            return JdbcQueryResultRow(resultSet!!)
        }
    }

    override fun close() {
        resultSet?.close()
    }
}