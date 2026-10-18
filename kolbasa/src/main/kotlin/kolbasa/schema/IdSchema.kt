package kolbasa.schema

import kolbasa.utils.Helpers
import kolbasa.utils.JdbcHelpers.inTransaction
import kolbasa.utils.JdbcHelpers.usePreparedStatement
import kolbasa.utils.JdbcHelpers.useStatement
import java.sql.Statement
import javax.sql.DataSource

internal object IdSchema {

    private const val NODE_ID_ALPHABET = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    private const val NODE_ID_DEFAULT_LENGTH = 16

    // q__node
    const val NODE_TABLE_NAME = Const.INTERNAL_KOLBASA_TABLE_PREFIX + "node"
    private const val STATUS_COLUMN_NAME = "status"
    private const val STATUS_COLUMN_LENGTH = 100
    const val ID_COLUMN_NAME = "id"
    const val ID_COLUMN_LENGTH = 100
    private const val CREATED_AT_COLUMN_NAME = "created_at"

    private const val IDENTIFIERS_BUCKET_COLUMN_NAME = "identifiers_bucket"

    private const val ACTIVE_STATUS = "active"

    private val CREATE_TABLE_STATEMENT = """
        create table if not exists $NODE_TABLE_NAME(
            $STATUS_COLUMN_NAME varchar($STATUS_COLUMN_LENGTH) not null primary key,
            $ID_COLUMN_NAME varchar($ID_COLUMN_LENGTH) not null,
            $CREATED_AT_COLUMN_NAME timestamp not null default current_timestamp,
            $IDENTIFIERS_BUCKET_COLUMN_NAME int not null
        )
    """.trimIndent()

    private val INSERT_STATEMENT = """
        insert into $NODE_TABLE_NAME($STATUS_COLUMN_NAME, $ID_COLUMN_NAME, $IDENTIFIERS_BUCKET_COLUMN_NAME)
        values ('$ACTIVE_STATUS', ?, ?)
        on conflict do nothing
    """.trimIndent()

    private val SELECT_ACTIVE_NODE_STATEMENT =
        "select $ID_COLUMN_NAME, $IDENTIFIERS_BUCKET_COLUMN_NAME from $NODE_TABLE_NAME where $STATUS_COLUMN_NAME='$ACTIVE_STATUS'"

    fun createAndInitIdTable(dataSource: DataSource, identifierBucket: Int = Node.MIN_BUCKET): Boolean {
        val nodeId = NodeId(Helpers.randomString(NODE_ID_DEFAULT_LENGTH, NODE_ID_ALPHABET))
        return createAndInitIdTable(dataSource, nodeId, identifierBucket)
    }

    /**
     * Creates `q__node` with [nodeId] and [identifierBucket], unless the table already exists
     *
     * Returns `true` if this call wrote the row. An existing table is left as it is, even an empty one.
     */
    fun createAndInitIdTable(dataSource: DataSource, nodeId: NodeId, identifierBucket: Int): Boolean {
        if (nodeTableExists(dataSource)) {
            return false
        }

        return dataSource.inTransaction { connection ->
            // One transaction, so a crash can't leave the table without its row
            connection.useStatement { statement ->
                statement.execute(CREATE_TABLE_STATEMENT)
            }

            connection.usePreparedStatement(INSERT_STATEMENT) { preparedStatement ->
                preparedStatement.setString(1, nodeId.id)
                preparedStatement.setInt(2, identifierBucket)
                preparedStatement.executeUpdate() > 0
            }
        }
    }

    /**
     * Whether `q__node` exists. An existing table can still have no active row, see [findNodeInfo]
     */
    fun nodeTableExists(dataSource: DataSource): Boolean {
        return SchemaExtractor.extractRawSchema(dataSource, setOf(NODE_TABLE_NAME)).containsKey(NODE_TABLE_NAME)
    }

    /**
     * The active row of `q__node`, or `null` if there is none. The table itself must exist
     */
    fun findNodeInfo(dataSource: DataSource): Node? {
        return dataSource.useStatement { statement: Statement ->
            statement.executeQuery(SELECT_ACTIVE_NODE_STATEMENT).use { resultSet ->
                if (resultSet.next()) {
                    Node(NodeId(resultSet.getString(1)), resultSet.getInt(2))
                } else {
                    null
                }
            }
        }
    }

    /**
     * The active row of `q__node`. Throws if there is none, unlike [findNodeInfo]
     */
    fun readNodeInfo(dataSource: DataSource): Node {
        return checkNotNull(findNodeInfo(dataSource)) {
            "Table $NODE_TABLE_NAME is empty, but it must contain at least one active entry. Did you forget to call " +
                "kolbasa.schema.SchemaHelpers.generateCreateOrUpdateStatements() method?  Query: '$SELECT_ACTIVE_NODE_STATEMENT'"
        }
    }

    fun updateIdentifiersBucket(dataSource: DataSource, oldBucket: Int, newBucket: Int): Boolean {
        val sql = """
            update $NODE_TABLE_NAME set $IDENTIFIERS_BUCKET_COLUMN_NAME=$newBucket
            where $STATUS_COLUMN_NAME='$ACTIVE_STATUS' and $IDENTIFIERS_BUCKET_COLUMN_NAME=$oldBucket"""

        val rowsUpdated = dataSource.useStatement { statement -> statement.executeUpdate(sql) }

        return rowsUpdated > 0
    }

}
