import kotlinx.cinterop.toKString
import pq.*

public fun main() {
    println("pq version: ${PQlibVersion()}")

    val conn = PQconnectdb("postgresql://kefir:kefir@localhost:8570/kefir")
        ?: throw RuntimeException("conn couldn't be established")
    try {
        println("conn status: ${PQstatus(conn)}")
        if (PQstatus(conn) != ConnStatusType.CONNECTION_OK) {
            throw RuntimeException("error during establishing conn: ${PQerrorMessage(conn)?.toKString()}")
        }
        println("DB version: ${PQserverVersion(conn)}")
        val result = PQexec(conn, "SELECT 1+2")
            ?: throw RuntimeException("query couldn't be executed")
        try {
            println("query status: ${PQresultStatus(result)}")
            if (PQresultStatus(result) != PGRES_TUPLES_OK) {
                throw RuntimeException("error during running query: ${PQerrorMessage(conn)?.toKString()}")
            }
            println("number of rows: #${PQntuples(result)}")
            println("number of columns: #${PQnfields(result)}")
            for (row in 0 until PQntuples(result)) {
                for (column in 0 until PQnfields(result)) {
                    println("value [$row, $column]: ${PQgetvalue(result, row, column)?.toKString()}")
                    println("type [$row, $column]: ${PQparamtype(result, column)}")
                    println("is null [$row, $column]: ${PQgetisnull(result, row, column)}")
                }
            }
        } finally {
            PQclear(result)
        }
    } finally {
        PQfinish(conn)
    }
}
