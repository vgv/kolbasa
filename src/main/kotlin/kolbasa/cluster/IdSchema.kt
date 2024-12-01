package kolbasa.cluster

import kolbasa.pg.DatabaseExtensions.useConnectionWithAutocommit
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.schema.Const
import java.sql.Statement
import javax.sql.DataSource

internal object IdSchema {

    private const val NODE_ID_ALPHABET = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    private const val NODE_ID_DEFAULT_LENGTH = 12

    private const val ID_TABLE_NAME = Const.QUEUE_TABLE_NAME_PREFIX + "_id"
    private const val ID_COLUMN_NAME = "id"
    private const val NODE_COLUMN_NAME = "node"
    const val NODE_COLUMN_LENGTH = 1000
    private const val CREATED_AT_COLUMN_NAME = "created_at"

    private const val ID_DEFAULT_VALUE = 1L

    private val CREATE_ID_TABLE_STATEMENT = """
        create table if not exists $ID_TABLE_NAME(
               $ID_COLUMN_NAME bigint not null primary key,
               $NODE_COLUMN_NAME varchar($NODE_COLUMN_LENGTH) not null,
               $CREATED_AT_COLUMN_NAME timestamp not null default current_timestamp
        )
    """.trimIndent()

    private val INIT_ID_TABLE_STATEMENT: String
        get() = """
                insert into $ID_TABLE_NAME
                    ($ID_COLUMN_NAME, $NODE_COLUMN_NAME)
                values
                    ($ID_DEFAULT_VALUE, '${generateNodeId()}')
                on conflict do nothing
            """.trimIndent()

    private val READ_NODE_ID_STATEMENT = """
        select
            $NODE_COLUMN_NAME
        from
            $ID_TABLE_NAME
        where
            $ID_COLUMN_NAME = 1
    """.trimIndent()

    fun createAndInitIdTable(dataSource: DataSource) {
        val ddlStatements = listOf(
            CREATE_ID_TABLE_STATEMENT,
            INIT_ID_TABLE_STATEMENT
        )

        dataSource.useConnectionWithAutocommit { connection ->
            // separate transaction for each statement
            connection.createStatement().use { statement ->
                ddlStatements.forEach { ddlStatement ->
                    statement.execute(ddlStatement)
                }
            }
        }
    }

    fun readNodeId(dataSource: DataSource): String? {
        return dataSource.useStatement { statement: Statement ->
            statement.executeQuery(READ_NODE_ID_STATEMENT).use { resultSet ->
                if (resultSet.next()) {
                    resultSet.getString(1)
                } else {
                    null
                }
            }
        }
    }

    private fun generateNodeId(): String {
        val sb = StringBuilder()
        for (i in 1..NODE_ID_DEFAULT_LENGTH) {
            sb.append(NODE_ID_ALPHABET.random())
        }
        return sb.toString()
    }
}
