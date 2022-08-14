package org.kefir.sql.libpq

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.kefir.sql.ColumnIndexOutOfBoundsException
import org.kefir.sql.ColumnWrongTypeException
import org.kefir.sql.Connection
import org.kefir.sql.getBoolean
import org.kefir.sql.getDouble
import org.kefir.sql.getFloat
import org.kefir.sql.getInt
import org.kefir.sql.getLong
import org.kefir.sql.getString
import org.kefir.sql.use
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class PostgreSQLConnectionTest {
    private lateinit var connection: Connection

    @BeforeTest
    fun `connect to PostgreSQL`() {
        connection = PostgreSQLConnection("postgresql://kefir:kefir@localhost:8570/kefir")
    }

    @AfterTest
    fun `disconnect from PostgreSQL`() {
        connection.close()
    }

    @Test
    fun `should correctly determine number of rows in query result`() {
        connection.query("SELECT 1 UNION SELECT 2").use { result ->
            val rows = result.toList()
            rows shouldHaveSize 2
        }
    }

    @Test
    fun `should throw NoSuchElementException on query result exhaustion`() {
        connection.query("SELECT 1").use { result ->
            val resultIt = result.iterator()
            resultIt.hasNext() shouldBe true

            resultIt.next()
            resultIt.hasNext() shouldBe false

            shouldThrow<NoSuchElementException> { resultIt.next() }
        }
    }

    @Test
    fun `should return zero rows for condition which is never true`() {
        connection.query("SELECT 1 WHERE 2 = 3").use { result ->
            val rows = result.toList()
            rows shouldHaveSize 0
        }
    }

    @Test
    fun `should correctly determine number of columns in query result`() {
        connection.query("SELECT 1, true, 'abc'").use { result ->
            val row = result.iterator().next()
            row.columns() shouldBe 3
        }
    }

    @Test
    fun `should throw ColumnIndexOutOfBoundsException if given column number is out of range`() {
        connection.query("SELECT 0, false, 'xyz'").use { result ->
            val row = result.iterator().next()

            shouldThrow<ColumnIndexOutOfBoundsException> { row.getBoolean(-1) }
            shouldThrow<ColumnIndexOutOfBoundsException> { row.getInt(-1) }
            shouldThrow<ColumnIndexOutOfBoundsException> { row.getLong(3) }
            shouldThrow<ColumnIndexOutOfBoundsException> { row.getString(3) }
        }
    }

    @Test
    fun `should correctly handle booleans in query result`() {
        connection.query("SELECT true, false").use { result ->
            val row = result.toList()[0]

            row.getBoolean(0) shouldBe true
            shouldThrow<ColumnWrongTypeException> { row.getInt(0) }
            shouldThrow<ColumnWrongTypeException> { row.getLong(0) }
            shouldThrow<ColumnWrongTypeException> { row.getString(0) }

            row.getBoolean(1) shouldBe false
            shouldThrow<ColumnWrongTypeException> { row.getInt(1) }
            shouldThrow<ColumnWrongTypeException> { row.getLong(1) }
            shouldThrow<ColumnWrongTypeException> { row.getString(1) }
        }
    }

    @Test
    fun `should correctly handle ints in query result`() {
        connection.query("SELECT 123::integer").use { result ->
            val row = result.toList()[0]

            row.getInt(0) shouldBe 123
            shouldThrow<ColumnWrongTypeException> { row.getBoolean(0) }
            shouldThrow<ColumnWrongTypeException> { row.getLong(0) }
            shouldThrow<ColumnWrongTypeException> { row.getString(0) }
        }
    }

    @Test
    fun `should correctly handle longs in query result`() {
        connection.query("SELECT 5678901234::bigint").use { result ->
            val row = result.toList()[0]

            row.getLong(0) shouldBe 5678901234
            shouldThrow<ColumnWrongTypeException> { row.getBoolean(0) }
            shouldThrow<ColumnWrongTypeException> { row.getInt(0) }
            shouldThrow<ColumnWrongTypeException> { row.getString(0) }
        }
    }

    @Test
    fun `should correctly handle floats in query result`() {
        connection.query("SELECT 12.34::float4").use { result ->
            val row = result.toList()[0]

            row.getFloat(0) shouldBe 12.34F
            shouldThrow<ColumnWrongTypeException> { row.getInt(0) }
            shouldThrow<ColumnWrongTypeException> { row.getDouble(0) }
            shouldThrow<ColumnWrongTypeException> { row.getString(0) }
        }
    }

    @Test
    fun `should correctly handle doubles in query result`() {
        connection.query("SELECT 56.78::float8").use { result ->
            val row = result.toList()[0]

            row.getDouble(0) shouldBe 56.78
            shouldThrow<ColumnWrongTypeException> { row.getLong(0) }
            shouldThrow<ColumnWrongTypeException> { row.getFloat(0) }
            shouldThrow<ColumnWrongTypeException> { row.getString(0) }
        }
    }

    @Test
    fun `should correctly process strings in query result`() {
        connection.query("SELECT 'abc'::varchar").use { result ->
            val row = result.toList()[0]

            row.getString(0) shouldBe "abc"
            shouldThrow<ColumnWrongTypeException> { row.getBoolean(0) }
            shouldThrow<ColumnWrongTypeException> { row.getInt(0) }
            shouldThrow<ColumnWrongTypeException> { row.getLong(0) }
        }
    }

    @Test
    fun `should correctly process nulls in query result`() {
        connection.query("SELECT null::text").use { result ->
            val row = result.toList()[0]

            row.getString(0) shouldBe null
            shouldThrow<ColumnWrongTypeException> { row.getBoolean(0) }
            shouldThrow<ColumnWrongTypeException> { row.getInt(0) }
            shouldThrow<ColumnWrongTypeException> { row.getLong(0) }
        }
    }

    @Test
    fun `should return given parameters for the same query`() {
        val queryString = "SELECT $1::int, $2::varchar"
        connection.query(queryString, 123, "abc").use { result ->
            val row = result.toList()[0]

            row.getInt(0) shouldBe 123
            row.getString(1) shouldBe "abc"
        }
        connection.query(queryString, 456, "def").use { result ->
            val row = result.toList()[0]

            row.getInt(0) shouldBe 456
            row.getString(1) shouldBe "def"
        }
    }

    @Test
    fun `should correctly determine number of rows affected by select`() {
        connection.query("SELECT 1 UNION SELECT 2").use { result ->
            result.rowsAffected() shouldBe 2
        }
    }

    @Test
    fun `should have two rows affected for double insert`() {
        shouldBeForTableXyz {
            connection.query("INSERT INTO xyz(name) VALUES($1),($2)", "abc", "def").use { result ->
                result.rowsAffected() shouldBe 2
            }
        }
    }

    @Test
    fun `should have zero rows affected by update of empty table`() {
        shouldBeForTableXyz {
            connection.query("UPDATE xyz SET name = $1 WHERE id = $2", "def", 1).use { result ->
                result.rowsAffected() shouldBe 0
            }
        }
    }

    @Test
    fun `should have zero rows affected by non-matching update`() {
        shouldBeForTableXyz {
            connection.query("INSERT INTO xyz(name) VALUES($1)", "abc").use {}
            connection.query("UPDATE xyz SET name = $1 WHERE id = $2", "def", 100).use { result ->
                result.rowsAffected() shouldBe 0
            }
        }
    }

    @Test
    fun `should have one row affected by matching update`() {
        shouldBeForTableXyz {
            connection.query("INSERT INTO xyz(name) VALUES($1)", "abc").use {}
            connection.query("UPDATE xyz SET name = $1 WHERE id = $2", "def", 1).use { result ->
                result.rowsAffected() shouldBe 1
            }
        }
    }

    @Test
    fun `should have two rows affected by full update`() {
        shouldBeForTableXyz {
            connection.query("INSERT INTO xyz(name) VALUES($1)", "abc").use {}
            connection.query("INSERT INTO xyz(name) VALUES($1)", "def").use {}
            connection.query("UPDATE xyz SET name = $1", "ghi").use { result ->
                result.rowsAffected() shouldBe 2
            }
        }
    }

    @Test
    fun `should have zero rows affected by delete on empty table`() {
        shouldBeForTableXyz {
            connection.query("DELETE FROM xyz WHERE id=$1", 1).use { result ->
                result.rowsAffected() shouldBe 0
            }
        }
    }

    @Test
    fun `should have zero rows affected by non-matching delete`() {
        shouldBeForTableXyz {
            connection.query("INSERT INTO xyz(name) VALUES($1)", "abc").use {}
            connection.query("DELETE FROM xyz WHERE id=$1", 100).use { result ->
                result.rowsAffected() shouldBe 0
            }
        }
    }

    @Test
    fun `should have one row affected by matching delete`() {
        shouldBeForTableXyz {
            connection.query("INSERT INTO xyz(name) VALUES($1)", "abc").use {}
            connection.query("DELETE FROM xyz WHERE id=$1", 1).use { result ->
                result.rowsAffected() shouldBe 1
            }
        }
    }

    @Test
    fun `should have two rows affected by full table delete`() {
        shouldBeForTableXyz {
            connection.query("INSERT INTO xyz(name) VALUES($1)", "abc").use {}
            connection.query("INSERT INTO xyz(name) VALUES($1)", "def").use {}
            connection.query("DELETE FROM xyz", 1).use { result ->
                result.rowsAffected() shouldBe 2
            }
        }
    }

    private fun shouldBeForTableXyz(assertions: () -> Unit) {
        connection.query("CREATE TABLE xyz(id serial PRIMARY KEY, name varchar(128))").use {  }
        try {
            assertions()
        } finally {
            connection.query("DROP TABLE xyz").use {  }
        }
    }
}
