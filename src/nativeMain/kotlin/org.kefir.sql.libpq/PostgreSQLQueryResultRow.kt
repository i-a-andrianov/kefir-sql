package org.kefir.sql.libpq

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.toKString
import org.kefir.sql.ColumnIndexOutOfBoundsException
import org.kefir.sql.ColumnNameNotFoundException
import org.kefir.sql.ColumnWrongTypeException
import org.kefir.sql.QueryResultRow
import org.kefir.sql.libpq.cinterop.PGresult
import org.kefir.sql.libpq.cinterop.PQfnumber
import org.kefir.sql.libpq.cinterop.PQftype
import org.kefir.sql.libpq.cinterop.PQgetisnull
import org.kefir.sql.libpq.cinterop.PQgetvalue
import org.kefir.sql.libpq.cinterop.PQnfields
import kotlin.reflect.KClass

internal class PostgreSQLQueryResultRow(
    private val result: CPointer<PGresult>,
    private val row: Int
) : QueryResultRow {
    override fun columns(): Int = PQnfields(result)

    override fun <T : Any> getValue(columnIndex: Int, type: KClass<T>): T? {
        if (columnIndex < 0) throw ColumnIndexOutOfBoundsException("column $columnIndex should be non-negative")
        if (columnIndex >= columns()) throw ColumnIndexOutOfBoundsException("column $columnIndex is too large: ${columns()} available")

        val rawValue = getRawValue(columnIndex)

        val columnType = getType(columnIndex)
        if (columnType != type) throw ColumnWrongTypeException("column $columnIndex isn't $type but $columnType")

        if (rawValue != null) return map(rawValue, type)
        return null
    }

    override fun <T : Any> getValue(columnName: String, type: KClass<T>): T? {
        val columnIndex = PQfnumber(result, columnName)
        if (columnIndex < 0) throw ColumnNameNotFoundException("column $columnName is not found in query result")
        return getValue(columnIndex, type)
    }

    private fun getRawValue(column: Int): String? {
        if (PQgetisnull(result, row, column) > 0) return null
        return PQgetvalue(result, row, column)!!.toKString()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> map(rawValue: String, type: KClass<T>): T {
        return when (type) {
            Boolean::class -> when (rawValue) {
                "t" -> true
                "f" -> false
                else -> throw IllegalStateException("unknown boolean value: $rawValue")
            }
            Int::class -> rawValue.toInt()
            Long::class -> rawValue.toLong()
            Float::class -> rawValue.toFloat()
            Double::class -> rawValue.toDouble()
            String::class -> rawValue
            else -> throw IllegalArgumentException("unknown type: $type")
        } as T
    }

    // TODO use "SELECT oid, typname FROM pg_type" query
    private fun getType(column: Int): KClass<out Any> {
        val typeOid = PQftype(result, column)
        return when (typeOid) {
            16U -> Boolean::class
            23U -> Int::class
            20U -> Long::class
            700U -> Float::class
            701U -> Double::class
            25U, 1043U -> String::class
            else -> throw RuntimeException("Unsupported data type for OId: $typeOid")
        }
    }
}
