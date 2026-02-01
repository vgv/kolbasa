package kolbasa.utils

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import javax.sql.DataSource

internal object JdbcHelpers {

    fun <T> DataSource.useConnection(block: (Connection) -> T): T {
        return connection.use { connection ->
            connection.autoCommit = false

            try {
                val result = block(connection)
                connection.commit()
                result
            } catch (e: Exception) {
                connection.rollback()
                throw e
            }
        }
    }

    fun <T> DataSource.useConnectionWithAutocommit(block: (Connection) -> T): T {
        return connection.use { connection ->
            connection.autoCommit = true

            try {
                val result = block(connection)
                result
            } catch (e: Exception) {
                throw e
            }
        }
    }

    // -------------------------------------------------------------------------------------------

    fun <T> Connection.useSavepoint(block: (Connection) -> T): Result<T> {
        val savepoint = this.setSavepoint()

        return try {
            val result = block(this)
            releaseSavepoint(savepoint)
            Result.success(result)
        } catch (e: Exception) {
            rollback(savepoint)
            Result.failure(e)
        }
    }

    // -------------------------------------------------------------------------------------------

    fun <T> DataSource.useStatement(block: (Statement) -> T): T {
        return useConnection { connection: Connection ->
            connection.useStatement(block)
        }
    }

    fun <T> Connection.useStatement(block: (Statement) -> T): T {
        return createStatement().use { statement: Statement ->
            block(statement)
        }
    }

    fun <T> DataSource.useStatement(query: String, block: (ResultSet) -> T): T {
        return useConnection { connection: Connection ->
            connection.useStatement(query, block)
        }
    }

    fun <T> Connection.useStatement(query: String, block: (ResultSet) -> T): T {
        return createStatement().use { statement: Statement ->
            statement.executeQuery(query).use { resultSet ->
                block(resultSet)
            }
        }
    }

    // -------------------------------------------------------------------------------------------

    fun <T> DataSource.usePreparedStatement(query: String, block: (PreparedStatement) -> T): T {
        return useConnection { connection: Connection ->
            connection.usePreparedStatement(query, block)
        }
    }

    fun <T> Connection.usePreparedStatement(query: String, block: (PreparedStatement) -> T): T {
        return prepareStatement(query).use { preparedStatement: PreparedStatement ->
            block(preparedStatement)
        }
    }

    // -------------------------------------------------------------------------------------------

    fun DataSource.readStringList(query: String): List<String> {
        return useConnection { connection ->
            connection.readStringList(query)
        }
    }

    fun Connection.readStringList(query: String): List<String> {
        val result = mutableListOf<String>()

        useStatement { statement ->
            statement.executeQuery(query).use { resultSet ->
                while (resultSet.next()) {
                    result += resultSet.getString(1)
                }
            }
        }

        return result
    }

    // -------------------------------------------------------------------------------------------

    fun DataSource.readIntList(query: String): List<Int> {
        return useConnection { connection ->
            connection.readIntList(query)
        }
    }

    fun Connection.readIntList(query: String): List<Int> {
        val result = mutableListOf<Int>()

        useStatement { statement ->
            statement.executeQuery(query).use { resultSet ->
                while (resultSet.next()) {
                    result += resultSet.getInt(1)
                }
            }
        }

        return result
    }

    // -------------------------------------------------------------------------------------------

    fun DataSource.readLongList(query: String): List<Long> {
        return useConnection { connection ->
            connection.readLongList(query)
        }
    }

    fun Connection.readLongList(query: String): List<Long> {
        val result = mutableListOf<Long>()

        useStatement { statement ->
            statement.executeQuery(query).use { resultSet ->
                while (resultSet.next()) {
                    result += resultSet.getLong(1)
                }
            }
        }

        return result
    }

    // -------------------------------------------------------------------------------------------
    fun DataSource.readInt(sql: String): Int {
        return useConnection { connection ->
            connection.readInt(sql)
        }
    }

    fun Connection.readInt(sql: String): Int {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                require(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getInt(1)

                // Do we have more rows than one?
                require(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                value
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    fun DataSource.readLong(sql: String): Long {
        return useConnection { connection ->
            connection.readLong(sql)
        }
    }

    fun Connection.readLong(sql: String): Long {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                require(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getLong(1)

                // Do we have more rows than one?
                require(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                value
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    fun DataSource.readBoolean(sql: String): Boolean {
        return useConnection { connection ->
            connection.readBoolean(sql)
        }
    }

    fun Connection.readBoolean(sql: String): Boolean {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                require(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getBoolean(1)

                // Do we have more rows than one?
                require(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                value
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    fun DataSource.readString(sql: String): String {
        return useConnection { connection ->
            connection.readString(sql)
        }
    }

    fun Connection.readString(sql: String): String {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                require(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getString(1)

                // Do we have more rows than one?
                require(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                value
            }
        }
    }

}
