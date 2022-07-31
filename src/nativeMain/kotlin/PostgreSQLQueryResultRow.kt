import kotlinx.cinterop.CPointer
import kotlinx.cinterop.toKString
import pq.PGresult
import pq.PQgetisnull
import pq.PQgetvalue
import pq.PQnfields

public class PostgreSQLQueryResultRow internal constructor(
    private val result: CPointer<PGresult>,
    private val row:Int
) {
    public fun columns(): Int = PQnfields(result)

    public fun getInt(column: Int): Int? = getValue(column)?.toInt()

    public fun getString(column: Int): String? = getValue(column)

    private fun getValue(column: Int): String? {
        if (column < 0) throw IllegalArgumentException("column should be non-negative: $column given")
        if (column >= columns()) throw IllegalArgumentException("column $column is too large: ${columns()} available")

        if (PQgetisnull(result, row, column) > 0) return null
        return PQgetvalue(result, row, column)!!.toKString()
    }
}
