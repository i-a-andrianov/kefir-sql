package org.kefir.sql.jdbc

import org.kefir.sql.QueryResultRow
import java.sql.ResultSet
import kotlin.reflect.KClass

internal class JdbcQueryResultRow(
    private val resultSet: ResultSet
) : QueryResultRow {
    override fun columns(): Int {
        var index = 0
        while (true) {
            try {
                resultSet.getObject(index + 1)
                index++
            } catch (e: Exception) {
                return index
            }
        }
    }

    override fun <T : Any> getValue(columnIndex: Int, type: KClass<T>): T? {
        return resultSet.getObject(columnIndex + 1, type.java)
    }

    override fun <T : Any> getValue(columnName: String, type: KClass<T>): T? {
        return resultSet.getObject(columnName, type.java)
    }
}
