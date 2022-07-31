import kotlinx.cinterop.CPointer
import kotlinx.cinterop.toKString
import pq.*

public class PostgreSQLConnection(url: String): Closeable {
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

    public fun query(queryString: String, vararg params: Any): PostgreSQLQueryResult {
        if (PQstatus(conn) != ConnStatusType.CONNECTION_OK) {
            PQreset(conn)
        }
        if (PQstatus(conn) != ConnStatusType.CONNECTION_OK) {
            throw RuntimeException("error during establishing connection: ${PQerrorMessage(conn)?.toKString()}")
        }
        return PostgreSQLQueryResult(conn, queryString, params)
    }

    public override fun close() {
        PQfinish(conn)
    }
}
