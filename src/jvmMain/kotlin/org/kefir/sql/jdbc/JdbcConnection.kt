package org.kefir.sql.jdbc

import org.kefir.sql.Connection
import org.kefir.sql.QueryResult
import java.sql.PreparedStatement
import javax.sql.DataSource

public class JdbcConnection(
    private val dataSource: DataSource
) : Connection {
    private val preparedStatementCache = mutableMapOf<String, PreparedStatement>()

    override fun query(queryString: String, vararg params: Any): QueryResult {
        val connection = dataSource.connection
        val statement = preparedStatementCache.computeIfAbsent(queryString, connection::prepareStatement)
        for ((idx, param) in params.withIndex()) {
            statement.setObject(idx, param)
        }
        val result = statement.execute()
        return JdbcQueryResult(
            if (result) statement.resultSet else null,
            if (result) null else statement.updateCount
        )
    }

    override fun close() {
        for (statement in preparedStatementCache.values) {
            statement.close()
        }
    }
}
