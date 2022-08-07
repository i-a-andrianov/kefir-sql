package org.kefir.sql

import kotlin.reflect.KClass

public interface QueryResultRow {
    public fun columns(): Int
    public fun <T : Any> getValue(column: Int, type: KClass<T>): T?
}

public fun QueryResultRow.getBoolean(column: Int): Boolean? = getValue(column)
public fun QueryResultRow.getInt(column: Int): Int? = getValue(column)
public fun QueryResultRow.getLong(column: Int): Long? = getValue(column)
public fun QueryResultRow.getFloat(column: Int): Float? = getValue(column)
public fun QueryResultRow.getDouble(column: Int): Double? = getValue(column)
public fun QueryResultRow.getString(column: Int): String? = getValue(column)

public inline fun <reified T : Any> QueryResultRow.getValue(column: Int): T? {
    return getValue(column, T::class)
}
