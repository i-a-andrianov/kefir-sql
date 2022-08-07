package org.kefir.sql.libpq

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.toCStringArray
import kotlinx.cinterop.toKString
import kotlinx.cinterop.value
import org.kefir.sql.QueryResult
import org.kefir.sql.QueryResultRow
import org.kefir.sql.libpq.cinterop.OidVar
import org.kefir.sql.libpq.cinterop.PGRES_COMMAND_OK
import org.kefir.sql.libpq.cinterop.PGRES_TUPLES_OK
import org.kefir.sql.libpq.cinterop.PGconn
import org.kefir.sql.libpq.cinterop.PGresult
import org.kefir.sql.libpq.cinterop.PQclear
import org.kefir.sql.libpq.cinterop.PQerrorMessage
import org.kefir.sql.libpq.cinterop.PQexecParams
import org.kefir.sql.libpq.cinterop.PQntuples
import org.kefir.sql.libpq.cinterop.PQresultStatus

internal class PostgreSQLQueryResult(
    conn: CPointer<PGconn>,
    queryString: String,
    params: Array<out Any>
) : QueryResult {
    private val result: CPointer<PGresult>

    init {
        result = memScoped {
            val paramTypes = allocArray<OidVar>(params.size) { idx: Int -> this.value = toOid(params[idx]) }
            val paramValues = params.map { it.toString() }.toCStringArray(this)
            PQexecParams(
                conn,
                queryString,
                params.size,
                paramTypes,
                paramValues,
                null,
                null,
                0
            ) ?: throw RuntimeException("query couldn't be executed")
        }

        if (PQresultStatus(result) !in setOf(PGRES_TUPLES_OK, PGRES_COMMAND_OK)) {
            close()
            throw RuntimeException("error during running query: ${PQerrorMessage(conn)?.toKString()}")
        }
    }

    // TODO use "SELECT oid, typname FROM pg_type" query
    private fun toOid(value: Any) = when (value) {
        is Boolean -> 16U
        is Int -> 23U
        is Long -> 20U
        is Float -> 700U
        is Double -> 701U
        is String -> 25U
        else -> throw IllegalArgumentException("Unsupported data type for value: $value")
    }

    override fun iterator(): Iterator<QueryResultRow> = ResultIterator()

    private inner class ResultIterator : Iterator<PostgreSQLQueryResultRow> {
        private var row = 0

        override fun hasNext(): Boolean = row < PQntuples(result)

        override fun next(): PostgreSQLQueryResultRow {
            if (!hasNext()) throw NoSuchElementException("no more elements")
            val resultRow = PostgreSQLQueryResultRow(result, row)
            row++
            return resultRow
        }
    }

    override fun close() {
        PQclear(result)
    }
}
