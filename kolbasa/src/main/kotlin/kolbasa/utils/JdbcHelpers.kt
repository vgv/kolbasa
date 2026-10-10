package kolbasa.utils

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import javax.sql.DataSource

object JdbcHelpers {

    /**
     * Runs [block] in one database transaction and commits it.
     *
     * Takes a connection from this [DataSource], sets `autoCommit` to `false`, calls [block] and commits. If [block] throws
     * anything, the transaction is rolled back and the exception is rethrown unchanged (a failing rollback is attached to it
     * as a suppressed exception, so the original failure is never lost). The connection is closed - returned to the pool -
     * in all cases.
     *
     * Two things worth knowing:
     * - `autoCommit` is set explicitly, on purpose. A connection from a pool arrives in whatever state the pool configured,
     *   and with `autoCommit = true` every statement would be committed on its own - your own insert would already be saved
     *   before the message is sent, and there would be nothing left to roll back.
     * - Nesting does not join transactions. This function always takes its own connection, so calling it inside another
     *   `inTransaction` block gives you a second, independent transaction, not a nested one. To share one transaction, pass
     *   the same [Connection] down.
     *
     * ## Usage Example
     *
     * ```kotlin
     * val producer = ConnectionAwareDatabaseProducer()
     *
     * dataSource.inTransaction { connection ->
     * // Your business code
     *     connection.prepareStatement("insert into orders(id, customer) values (?, ?)").use { statement ->
     *         statement.setLong(1, orderId)
     *         statement.setString(2, customer)
     *         statement.executeUpdate()
     *     }
     *
     *     // The same connection, so the same transaction: if the insert above is rolled back, this message is not sent
     *     producer.send(connection, queue, "Order $orderId was created")
     * }
     * ```
     *
     * @param block work to do inside the transaction; the connection it receives is open until the block returns
     * @return whatever [block] returned
     * @see withAutoCommit
     */
    @JvmStatic
    @Throws(SQLException::class)
    fun <T> DataSource.inTransaction(block: java.util.function.Function<Connection, T>): T {
        return connection.use { connection ->
            connection.autoCommit = false

            try {
                val result = block.apply(connection)
                connection.commit()
                result
            } catch (e: Throwable) {
                try {
                    connection.rollback()
                } catch (rollbackError: Throwable) {
                    e.addSuppressed(rollbackError)
                }

                throw e
            }
        }
    }

    /**
     * Runs [block] on a connection with `autoCommit` turned on, so every statement is committed on its own.
     *
     * Takes a connection from this [DataSource], sets `autoCommit` to `true` and calls [block]. There is no transaction to
     * commit or to roll back: each statement is final the moment it succeeds, and a statement that fails leaves the
     * statements before it in place. The connection is closed - returned to the pool - in all cases.
     *
     * Use this for a series of independent statements, where a later failure should not undo what already succeeded. Do
     * **not** use it when a send or a receive has to be atomic with your own data - that is what [inTransaction] is for, and
     * it is the whole reason the `ConnectionAware*` roles exist.
     *
     * `autoCommit` is set explicitly here as well, for the same reason as in [inTransaction]: a pooled connection arrives in
     * whatever state the pool configured, and a helper that inherited that state would behave differently depending on the
     * pool. Note that the connection is handed back to the pool with `autoCommit = true`; pools normally reset connection
     * state when a connection is returned, but it is worth knowing if yours does not.
     *
     * ## Usage Example
     *
     * ```kotlin
     * // Each statement is committed on its own, so a failure in the middle keeps the earlier ones
     * dataSource.withAutoCommit { connection ->
     *     connection.createStatement().use { statement ->
     *         statement.execute("vacuum analyze q_orders")
     *         statement.execute("vacuum analyze q_customers")
     *     }
     * }
     * ```
     *
     * @param block work to do on the connection; the connection it receives is open until the block returns
     * @return whatever [block] returned
     * @see inTransaction
     */
    @JvmStatic
    fun <T> DataSource.withAutoCommit(block: java.util.function.Function<Connection, T>): T {
        return useConnectionWithAutocommit(block::apply)
    }

    internal fun <T> DataSource.useConnectionWithAutocommit(block: (Connection) -> T): T {
        return connection.use { connection ->
            connection.autoCommit = true
            block(connection)
        }
    }

    // -------------------------------------------------------------------------------------------

    internal fun <T> Connection.useSavepoint(block: (Connection) -> T): Result<T> {
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

    internal fun <T> DataSource.useStatement(block: (Statement) -> T): T {
        return inTransaction { connection: Connection ->
            connection.useStatement(block)
        }
    }

    internal fun <T> Connection.useStatement(block: (Statement) -> T): T {
        return createStatement().use { statement: Statement ->
            block(statement)
        }
    }

    internal fun <T> DataSource.useStatement(query: String, block: (ResultSet) -> T): T {
        return inTransaction { connection: Connection ->
            connection.useStatement(query, block)
        }
    }

    internal fun <T> Connection.useStatement(query: String, block: (ResultSet) -> T): T {
        return createStatement().use { statement: Statement ->
            statement.executeQuery(query).use { resultSet ->
                block(resultSet)
            }
        }
    }

    // -------------------------------------------------------------------------------------------

    internal fun <T> DataSource.usePreparedStatement(query: String, block: (PreparedStatement) -> T): T {
        return inTransaction { connection: Connection ->
            connection.usePreparedStatement(query, block)
        }
    }

    internal fun <T> Connection.usePreparedStatement(query: String, block: (PreparedStatement) -> T): T {
        return prepareStatement(query).use { preparedStatement: PreparedStatement ->
            block(preparedStatement)
        }
    }

    // -------------------------------------------------------------------------------------------

    internal fun DataSource.readStringList(query: String): List<String> {
        return inTransaction { connection ->
            connection.readStringList(query)
        }
    }

    internal fun Connection.readStringList(query: String): List<String> {
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

    internal fun DataSource.readIntList(query: String): List<Int> {
        return inTransaction { connection ->
            connection.readIntList(query)
        }
    }

    internal fun Connection.readIntList(query: String): List<Int> {
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

    internal fun DataSource.readLongList(query: String): List<Long> {
        return inTransaction { connection ->
            connection.readLongList(query)
        }
    }

    internal fun Connection.readLongList(query: String): List<Long> {
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
    internal fun DataSource.readInt(sql: String): Int {
        return inTransaction { connection ->
            connection.readInt(sql)
        }
    }

    internal fun Connection.readInt(sql: String): Int {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                check(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getInt(1)

                // Do we have more rows than one?
                check(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                value
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    internal fun DataSource.readLong(sql: String): Long {
        return inTransaction { connection ->
            connection.readLong(sql)
        }
    }

    internal fun Connection.readLong(sql: String): Long {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                check(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getLong(1)

                // Do we have more rows than one?
                check(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                value
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    internal fun DataSource.readLongOrNull(sql: String): Long? {
        return inTransaction { connection ->
            connection.readLongOrNull(sql)
        }
    }

    internal fun Connection.readLongOrNull(sql: String): Long? {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                check(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getLong(1)
                // Distinguish a real value from a SQL NULL (getLong returns 0 for both)
                val result = if (resultSet.wasNull()) null else value

                // Do we have more rows than one?
                check(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                result
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    internal fun DataSource.readBoolean(sql: String): Boolean {
        return inTransaction { connection ->
            connection.readBoolean(sql)
        }
    }

    internal fun Connection.readBoolean(sql: String): Boolean {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                check(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getBoolean(1)

                // Do we have more rows than one?
                check(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                value
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    internal fun DataSource.readString(sql: String): String {
        return inTransaction { connection ->
            connection.readString(sql)
        }
    }

    internal fun Connection.readString(sql: String): String {
        return useStatement { statement ->
            statement.executeQuery(sql).use { resultSet ->
                check(resultSet.next()) {
                    "No rows in the query '$sql'"
                }

                val value = resultSet.getString(1)

                // Do we have more rows than one?
                check(!resultSet.next()) {
                    "More than one row in the query '$sql'"
                }

                value
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    internal fun schemaNameOrDefault(schemaName: String?): String {
        return if (schemaName.isNullOrBlank()) {
            "public"
        } else {
            schemaName
        }
    }

}
