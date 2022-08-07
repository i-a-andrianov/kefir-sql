package org.kefir.sql.libpq

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.toKString
import org.kefir.sql.Connection
import org.kefir.sql.QueryResult
import org.kefir.sql.libpq.cinterop.ConnStatusType
import org.kefir.sql.libpq.cinterop.PGconn
import org.kefir.sql.libpq.cinterop.PQconnectdb
import org.kefir.sql.libpq.cinterop.PQerrorMessage
import org.kefir.sql.libpq.cinterop.PQfinish
import org.kefir.sql.libpq.cinterop.PQlibVersion
import org.kefir.sql.libpq.cinterop.PQreset
import org.kefir.sql.libpq.cinterop.PQserverVersion
import org.kefir.sql.libpq.cinterop.PQstatus

internal class PostgreSQLConnection(url: String) : Connection {
    private val conn: CPointer<PGconn>

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
        return PostgreSQLQueryResult(conn, queryString, params)
    }

    override fun close() {
        PQfinish(conn)
    }
}
