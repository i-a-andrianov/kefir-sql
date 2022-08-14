package org.kefir.sql.libpq

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.toKString
import kotlinx.cinterop.value
import org.kefir.sql.Connection
import org.kefir.sql.QueryResult
import org.kefir.sql.libpq.cinterop.ConnStatusType
import org.kefir.sql.libpq.cinterop.OidVar
import org.kefir.sql.libpq.cinterop.PGRES_COMMAND_OK
import org.kefir.sql.libpq.cinterop.PGRES_TUPLES_OK
import org.kefir.sql.libpq.cinterop.PGconn
import org.kefir.sql.libpq.cinterop.PQclear
import org.kefir.sql.libpq.cinterop.PQconnectdb
import org.kefir.sql.libpq.cinterop.PQerrorMessage
import org.kefir.sql.libpq.cinterop.PQfinish
import org.kefir.sql.libpq.cinterop.PQlibVersion
import org.kefir.sql.libpq.cinterop.PQprepare
import org.kefir.sql.libpq.cinterop.PQreset
import org.kefir.sql.libpq.cinterop.PQresultStatus
import org.kefir.sql.libpq.cinterop.PQserverVersion
import org.kefir.sql.libpq.cinterop.PQstatus

internal class PostgreSQLConnection(url: String) : Connection {
    private val conn: CPointer<PGconn>
    private val preparedQueryCache = mutableMapOf<String, String>()

    init {
        println("pq version: ${PQlibVersion()}")

        conn = PQconnectdb(url)
            ?: throw RuntimeException("connection couldn't be established")

        if (PQstatus(conn) != ConnStatusType.CONNECTION_OK) {
            close()
            throw RuntimeException("error during establishing connection: ${PQerrorMessage(conn)?.toKString()}")
        }

        println("PostgreSQL version: ${PQserverVersion(conn)}")
    }

    override fun query(queryString: String, vararg params: Any): QueryResult {
        if (PQstatus(conn) != ConnStatusType.CONNECTION_OK) {
            PQreset(conn)
        }
        if (PQstatus(conn) != ConnStatusType.CONNECTION_OK) {
            throw RuntimeException("error during establishing connection: ${PQerrorMessage(conn)?.toKString()}")
        }
        val queryId = preparedQueryId(queryString, params)
        return PostgreSQLQueryResult(conn, queryId, params)
    }

    private fun preparedQueryId(queryString: String, params: Array<out Any>): String {
        var queryId = preparedQueryCache[queryString]
        if (queryId == null) {
            queryId = preparedQueryCache.size.toString()
            prepareQuery(queryId, queryString, params)
            preparedQueryCache[queryString] = queryId
        }
        return queryId
    }

    private fun prepareQuery(queryId: String, queryString: String, params: Array<out Any>) {
        val result = memScoped {
            val paramTypes = allocArray<OidVar>(params.size) { idx: Int -> this.value = toOid(params[idx]) }

            PQprepare(conn, queryId, queryString, params.size, paramTypes)
                ?: throw RuntimeException("query couldn't be prepared")
        }

        try {
            if (PQresultStatus(result) !in setOf(PGRES_TUPLES_OK, PGRES_COMMAND_OK)) {
                throw RuntimeException("error during preparing query: ${PQerrorMessage(conn)?.toKString()}")
            }
        } finally {
            PQclear(result)
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

    override fun close() {
        PQfinish(conn)
    }
}
