import org.kefir.sql.getInt
import org.kefir.sql.getLong
import org.kefir.sql.getString
import org.kefir.sql.libpq.PostgreSQLConnection
import org.kefir.sql.use

public fun main() {
    PostgreSQLConnection("postgresql://kefir:kefir@localhost:8570/kefir").use { conn ->
        conn.query("SELECT 1+2, $1, null::bigint, '{\"hello\":\"world\"}'", "abc").use { result ->
            for ((idx, row) in result.withIndex()) {
                println("number of columns: #${row.columns()}")
                println("int[$idx, 0]: ${row.getInt(0)}")
                println("string[$idx, 1]: ${row.getString(1)}")
                println("int[$idx, 2]: ${row.getLong(2)}")
                println("string[$idx, 3]: ${row.getString(3)}")
            }
        }
        conn.query("CREATE TABLE xyz(id serial PRIMARY KEY, name varchar(128))").use {}
        conn.query("INSERT INTO xyz(name) VALUES($1) RETURNING id, name, 40+2::bigint", "abc").use { result ->
            println("id=${result.iterator().next().getInt(0)}")
            println("name=${result.iterator().next().getString(1)}")
            println("40+2=${result.iterator().next().getLong(2)}")
        }
        conn.query("DROP TABLE xyz").use {}
    }
}
