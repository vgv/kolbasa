package kolbasa.cluster.schema

import kolbasa.cluster.Shard
import kolbasa.producer.ShardId
import kolbasa.schema.Const
import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaExtractor
import kolbasa.utils.JdbcHelpers.useStatement
import kolbasa.utils.JdbcHelpers.withAutoCommit
import java.sql.Statement
import java.util.*
import javax.sql.DataSource

internal object ShardSchema {

    // q__shard
    const val SHARD_TABLE_NAME = Const.INTERNAL_KOLBASA_TABLE_PREFIX + "shard"
    const val SHARD_COLUMN_NAME = "shard"
    const val PRODUCER_NODE_COLUMN_NAME = "producer_node"
    const val CONSUMER_NODE_COLUMN_NAME = "consumer_node"
    const val NEXT_CONSUMER_NODE_COLUMN_NAME = "next_consumer_node"

    const val CREATE_SHARD_TABLE_STATEMENT = """
        create table if not exists $SHARD_TABLE_NAME(
            $SHARD_COLUMN_NAME int not null primary key,
            $PRODUCER_NODE_COLUMN_NAME varchar(${IdSchema.ID_COLUMN_LENGTH}) not null,
            $CONSUMER_NODE_COLUMN_NAME varchar(${IdSchema.ID_COLUMN_LENGTH}),
            $NEXT_CONSUMER_NODE_COLUMN_NAME varchar(${IdSchema.ID_COLUMN_LENGTH}),
            check (
                ($PRODUCER_NODE_COLUMN_NAME=$CONSUMER_NODE_COLUMN_NAME and $NEXT_CONSUMER_NODE_COLUMN_NAME is null) or
                ($PRODUCER_NODE_COLUMN_NAME=$NEXT_CONSUMER_NODE_COLUMN_NAME and $CONSUMER_NODE_COLUMN_NAME is null)
            )
        )
    """

    private const val READ_SHARD_TABLE_STATEMENT = """
        select
            $SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME, $NEXT_CONSUMER_NODE_COLUMN_NAME
        from
            $SHARD_TABLE_NAME
        where
            $SHARD_COLUMN_NAME between ${ShardId.MIN_SHARD} and ${ShardId.MAX_SHARD}
        order by
            $SHARD_COLUMN_NAME
    """

    /**
     * Creates `q__shard`, unless it already exists
     */
    fun createShardTable(dataSource: DataSource) {
        try {
            createShardTableOnce(dataSource)
        } catch (_: Exception) {
            // Another client created the table at the same moment, and our create lost on the catalog.
            // Its table is committed by now, so the second try finds it and does nothing
            createShardTableOnce(dataSource)
        }
    }

    private fun createShardTableOnce(dataSource: DataSource) {
        val ddlStatements = listOf(
            CREATE_SHARD_TABLE_STATEMENT,
        )

        dataSource.withAutoCommit { connection ->
            // separate transaction for each statement
            connection.createStatement().use { statement ->
                ddlStatements.forEach { ddlStatement ->
                    statement.execute(ddlStatement)
                }
            }
        }
    }

    fun fillShardTable(dataSource: DataSource, nodes: List<Node>) {
        val shardsPerStatement = 100
        val statements = ShardId.SHARDS_RANGE.chunked(shardsPerStatement).map { shards ->
            val values = shards.map { shard ->
                val randomNode = nodes.random()
                val randomNodeStringId: String = randomNode.id.id
                "($shard, '$randomNodeStringId', '$randomNodeStringId')"
            }

            """
                insert into $SHARD_TABLE_NAME
                    ($SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME)
                values
                    ${values.joinToString(",")}
                on conflict do nothing
            """.trimIndent()
        }

        dataSource.useStatement { statement ->
            statements.forEach { statement.execute(it) }
        }
    }

    /**
     * Whether `q__shard` exists. The catalog answers it, so a failed read is never taken for a missing table
     */
    fun shardTableExists(dataSource: DataSource): Boolean {
        return SchemaExtractor.extractRawSchema(dataSource, setOf(SHARD_TABLE_NAME)).containsKey(SHARD_TABLE_NAME)
    }

    /**
     * Looks for the shard table on [nodes], in node order, and stops at the first one that has every shard.
     *
     * A node whose check or read fails stops the search with an exception that names the node. A failed read is
     * not a missing table.
     */
    fun findShardTable(nodes: SortedMap<Node, DataSource>): ShardTableSearch {
        var firstIncomplete: ShardTableSearch.Incomplete? = null

        for ((node, dataSource) in nodes) {
            val shards = try {
                if (shardTableExists(dataSource))
                    readShards(dataSource)
                else
                    null
            } catch (e: Exception) {
                throw IllegalStateException("Can't read $SHARD_TABLE_NAME on node ${node.id.id}", e)
            }

            if (shards == null) {
                continue
            }

            if (shards.size == ShardId.SHARD_COUNT) {
                return ShardTableSearch.Complete(node, dataSource, shards)
            }

            // A complete table on a later node still wins, so go on
            if (firstIncomplete == null) {
                firstIncomplete = ShardTableSearch.Incomplete(node, dataSource, shards.size)
            }
        }

        // If there isn’t a single incomplete shard table - there is no shard table at all
        return firstIncomplete ?: ShardTableSearch.Missing
    }

    fun readShards(dataSource: DataSource): Map<ShardId, Shard> {
        val shards = hashMapOf<ShardId, Shard>()

        dataSource.useStatement { statement: Statement ->
            statement.executeQuery(READ_SHARD_TABLE_STATEMENT).use { resultSet ->
                while (resultSet.next()) {
                    val shard = ShardId.of(resultSet.getInt(SHARD_COLUMN_NAME))
                    val producerNode = NodeId(resultSet.getString(PRODUCER_NODE_COLUMN_NAME))
                    val consumerNode = resultSet.getString(CONSUMER_NODE_COLUMN_NAME)?.let { NodeId(it) }
                    val nextConsumerNode = resultSet.getString(NEXT_CONSUMER_NODE_COLUMN_NAME)?.let { NodeId(it) }

                    shards[shard] = Shard(
                        shard = shard,
                        producerNode = producerNode,
                        consumerNode = consumerNode,
                        nextConsumerNode = nextConsumerNode
                    )
                }
            }
        }

        return shards
    }

}
