package org.kefir.sql

import kotlin.reflect.KClass

public interface QueryResultRow {
    public fun columns(): Int
    public fun <T : Any> getValue(columnIndex: Int, type: KClass<T>): T?
    public fun <T : Any> getValue(columnName: String, type: KClass<T>): T?
}

public fun QueryResultRow.getBoolean(columnIndex: Int): Boolean? = getValue(columnIndex)
public fun QueryResultRow.getInt(columnIndex: Int): Int? = getValue(columnIndex)
public fun QueryResultRow.getLong(columnIndex: Int): Long? = getValue(columnIndex)
public fun QueryResultRow.getFloat(columnIndex: Int): Float? = getValue(columnIndex)
public fun QueryResultRow.getDouble(columnIndex: Int): Double? = getValue(columnIndex)
public fun QueryResultRow.getString(columnIndex: Int): String? = getValue(columnIndex)

public inline fun <reified T : Any> QueryResultRow.getValue(columnIndex: Int): T? {
    return getValue(columnIndex, T::class)
}

public fun QueryResultRow.getBoolean(columnName: String): Boolean? = getValue(columnName)
public fun QueryResultRow.getInt(columnName: String): Int? = getValue(columnName)
public fun QueryResultRow.getLong(columnName: String): Long? = getValue(columnName)
public fun QueryResultRow.getFloat(columnName: String): Float? = getValue(columnName)
public fun QueryResultRow.getDouble(columnName: String): Double? = getValue(columnName)
public fun QueryResultRow.getString(columnName: String): String? = getValue(columnName)

public inline fun <reified T : Any> QueryResultRow.getValue(columnName: String): T? {
    return getValue(columnName, T::class)
}
