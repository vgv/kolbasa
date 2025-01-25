package kolbasa.cluster.schema

import kolbasa.pg.DatabaseExtensions.useConnectionWithAutocommit
import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.schema.Const
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
    private const val SERVER_ID_COLUMN_NAME = "server_id"
    internal const val SERVER_ID_COLUMN_LENGTH = 100
    private const val CREATED_AT_COLUMN_NAME = "created_at"

    private const val IDENTIFIERS_BUCKET_COLUMN_NAME = "identifiers_bucket"

    // TODO: drop after a few releases
    private const val SEND_ENABLED_COLUMN_NAME = "send_enabled"
    private const val RECEIVE_ENABLED_COLUMN_NAME = "receive_enabled"

    private const val ACTIVE_STATUS = "active"

    private val CREATE_TABLE_STATEMENT = """
        create table if not exists $NODE_TABLE_NAME(
               $STATUS_COLUMN_NAME varchar($STATUS_COLUMN_LENGTH) not null primary key,
               $SERVER_ID_COLUMN_NAME varchar($SERVER_ID_COLUMN_LENGTH) not null,
               $CREATED_AT_COLUMN_NAME timestamp not null default current_timestamp,
               $IDENTIFIERS_BUCKET_COLUMN_NAME int
        )
    """.trimIndent()

    private val INIT_TABLE_STATEMENT: String
        get() = """
                insert into $NODE_TABLE_NAME
                    ($STATUS_COLUMN_NAME, $SERVER_ID_COLUMN_NAME)
                values
                    ('$ACTIVE_STATUS', '${generateNodeId()}')
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
            $STATUS_COLUMN_NAME = '$ACTIVE_STATUS'
    """.trimIndent()

    fun createAndInitIdTable(dataSource: DataSource) {
        val ddlStatements = listOf(
            CREATE_TABLE_STATEMENT,
            INIT_TABLE_STATEMENT,
            // TODO: drop after a few releases
            "alter table $NODE_TABLE_NAME add column if not exists $IDENTIFIERS_BUCKET_COLUMN_NAME int",
            "alter table $NODE_TABLE_NAME drop column if exists $SEND_ENABLED_COLUMN_NAME",
            "alter table $NODE_TABLE_NAME drop column if exists $RECEIVE_ENABLED_COLUMN_NAME",
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

    fun readNodeInfo(dataSource: DataSource): Node {
        val node = dataSource.useStatement { statement: Statement ->
            statement.executeQuery(SELECT_NODE_INFO_STATEMENT).use { resultSet ->
                if (resultSet.next()) {
                    val serverId = resultSet.getString(1)
                    var identifierBucket: Int? = resultSet.getInt(2)
                    if (resultSet.wasNull()) {
                        identifierBucket = null
                    }

                    Node(serverId, identifierBucket)
                } else {
                    null
                }
            }
        }

        return requireNotNull(node) {
            "Node info not found, maybe you forgot to initialize the cluster?"
        }
    }

    fun updateIdentifiersBucket(dataSource: DataSource, bucket: Int?) {
        dataSource.usePreparedStatement(UPDATE_NODE_INFO_STATEMENT) { preparedStatement: PreparedStatement ->
            if (bucket == null) {
                preparedStatement.setNull(1, java.sql.Types.INTEGER)
            } else {
                preparedStatement.setInt(1, bucket)
            }

            preparedStatement.executeUpdate()
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
