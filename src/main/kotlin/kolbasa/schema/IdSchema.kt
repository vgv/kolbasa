package kolbasa.schema

import kolbasa.utils.Helpers
import kolbasa.utils.JdbcHelpers.useConnectionWithAutocommit
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

    fun createAndInitIdTable(dataSource: DataSource, identifierBucket: Int = Node.MIN_BUCKET) {
        val ddlStatements = mutableListOf<String>()

        val existingTable = SchemaExtractor.extractRawSchema(dataSource, setOf(NODE_TABLE_NAME))[NODE_TABLE_NAME]
        if (existingTable == null) {
            ddlStatements += CREATE_TABLE_STATEMENT

            ddlStatements += """
                insert into $NODE_TABLE_NAME($STATUS_COLUMN_NAME, $ID_COLUMN_NAME, $IDENTIFIERS_BUCKET_COLUMN_NAME)
                values
                    ('$ACTIVE_STATUS', '${Helpers.randomString(NODE_ID_DEFAULT_LENGTH, NODE_ID_ALPHABET)}', $identifierBucket)
                on conflict do nothing
            """
        }

        // remove after few releases
        if (existingTable != null) {
            val idColumn = existingTable.findColumn(ID_COLUMN_NAME)
            if (idColumn != null && idColumn.nullable) {
                ddlStatements += "alter table $NODE_TABLE_NAME alter $ID_COLUMN_NAME set not null"
            }
        }

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
        val sql =
            "select $ID_COLUMN_NAME, $IDENTIFIERS_BUCKET_COLUMN_NAME from $NODE_TABLE_NAME where $STATUS_COLUMN_NAME='$ACTIVE_STATUS'"

        return dataSource.useStatement { statement: Statement ->
            statement.executeQuery(sql).use { resultSet ->
                require(resultSet.next()) {
                    "Table $NODE_TABLE_NAME is empty, but it must contain at least one active entry. Did you forget to call " +
                        "kolbasa.schema.SchemaHelpers.generateCreateOrUpdateStatements() method?  Query: '$sql'"
                }

                val nodeId = resultSet.getString(1)
                val identifiersBucket: Int = resultSet.getInt(2)

                Node(NodeId(nodeId), identifiersBucket)
            }
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
