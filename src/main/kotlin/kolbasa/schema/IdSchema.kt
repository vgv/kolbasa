package kolbasa.schema

import kolbasa.pg.DatabaseExtensions.useConnectionWithAutocommit
import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useStatement
import java.sql.PreparedStatement
import java.sql.Statement
import javax.sql.DataSource

internal object IdSchema {

    private const val NODE_ID_ALPHABET = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    private const val NODE_ID_DEFAULT_LENGTH = 12

    // q__node
    const val NODE_TABLE_NAME = Const.INTERNAL_KOLBASA_TABLE_PREFIX + "node"
    private const val STATUS_COLUMN_NAME = "status"
    private const val STATUS_COLUMN_LENGTH = 100
    const val SERVER_ID_COLUMN_NAME = "server_id"
    const val SERVER_ID_COLUMN_LENGTH = 100
    const val ID_COLUMN_NAME = "id"
    const val ID_COLUMN_LENGTH = 100
    private const val CREATED_AT_COLUMN_NAME = "created_at"

    private const val IDENTIFIERS_BUCKET_COLUMN_NAME = "identifiers_bucket"

    private const val ACTIVE_STATUS = "active"

    private val CREATE_TABLE_STATEMENT = """
        create table if not exists $NODE_TABLE_NAME(
               $STATUS_COLUMN_NAME varchar($STATUS_COLUMN_LENGTH) not null primary key,
               $SERVER_ID_COLUMN_NAME varchar($SERVER_ID_COLUMN_LENGTH),
               $ID_COLUMN_NAME varchar($ID_COLUMN_LENGTH),
               $CREATED_AT_COLUMN_NAME timestamp not null default current_timestamp,
               $IDENTIFIERS_BUCKET_COLUMN_NAME int not null
        )
    """.trimIndent()

    private val INIT_TABLE_STATEMENT: String
        get() = """
                insert into $NODE_TABLE_NAME
                    ($STATUS_COLUMN_NAME, $SERVER_ID_COLUMN_NAME, $IDENTIFIERS_BUCKET_COLUMN_NAME)
                values
                    ('$ACTIVE_STATUS', '${generateNodeId()}', ${Node.randomBucket()})
                on conflict do nothing
            """.trimIndent()

    private val SELECT_NODE_INFO_STATEMENT = """
        select
            $SERVER_ID_COLUMN_NAME, $IDENTIFIERS_BUCKET_COLUMN_NAME
        from
            $NODE_TABLE_NAME
        where
            $STATUS_COLUMN_NAME = '$ACTIVE_STATUS'
    """.trimIndent()

    private val UPDATE_NODE_INFO_STATEMENT = """
        update
            $NODE_TABLE_NAME
        set
            $IDENTIFIERS_BUCKET_COLUMN_NAME = ?
        where
            $STATUS_COLUMN_NAME = '$ACTIVE_STATUS' and
            $IDENTIFIERS_BUCKET_COLUMN_NAME = ?
    """.trimIndent()

    fun createAndInitIdTable(dataSource: DataSource) {
        val ddlStatements = listOf(
            CREATE_TABLE_STATEMENT,
            "alter table $NODE_TABLE_NAME alter $SERVER_ID_COLUMN_NAME drop not null",
            "alter table $NODE_TABLE_NAME add column if not exists $ID_COLUMN_NAME varchar($ID_COLUMN_LENGTH)",
            "update $NODE_TABLE_NAME set $ID_COLUMN_NAME=$SERVER_ID_COLUMN_NAME where ($ID_COLUMN_NAME <> $SERVER_ID_COLUMN_NAME) or ($ID_COLUMN_NAME is null)",
            INIT_TABLE_STATEMENT,
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

    fun  readNodeInfo(dataSource: DataSource): Node? {
        try {
            return dataSource.useStatement { statement: Statement ->
                statement.executeQuery(SELECT_NODE_INFO_STATEMENT).use { resultSet ->
                    if (resultSet.next()) {
                        val nodeId = resultSet.getString(1)
                        val identifiersBucket: Int = resultSet.getInt(2)

                        Node(NodeId(nodeId), identifiersBucket)
                    } else {
                        null
                    }
                }
            }
        } catch (_: Exception) {
            // exception doesn't matter
            return null
        }
    }

    fun updateIdentifiersBucket(dataSource: DataSource, oldBucket: Int, newBucket: Int): Boolean {
        val rowsUpdated = dataSource.usePreparedStatement(UPDATE_NODE_INFO_STATEMENT) { preparedStatement: PreparedStatement ->
            preparedStatement.setInt(1, newBucket)
            preparedStatement.setInt(2, oldBucket)

            preparedStatement.executeUpdate()
        }

        return rowsUpdated > 0
    }

    private fun generateNodeId(): String {
        val sb = StringBuilder(NODE_ID_DEFAULT_LENGTH)
        (1..NODE_ID_DEFAULT_LENGTH).forEach { _ ->
            sb.append(NODE_ID_ALPHABET.random())
        }
        return sb.toString()
    }
}
