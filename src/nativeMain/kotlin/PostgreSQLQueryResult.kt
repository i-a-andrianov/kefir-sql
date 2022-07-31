import kotlinx.cinterop.*
import pq.*

public class PostgreSQLQueryResult internal constructor(
    conn: CPointer<PGconn>,
    queryString: String,
    params: Array<out Any>
) : Iterable<PostgreSQLQueryResultRow>, Closeable {
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
        is Int -> 23U
        is String -> 1043U
        else -> throw RuntimeException("Unsupported data type for value: $value")
    }

    override fun iterator(): Iterator<PostgreSQLQueryResultRow> = ResultIterator()

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

    public override fun close() {
        PQclear(result)
    }
}
