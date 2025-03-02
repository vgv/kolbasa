package kolbasa.cluster.migrate

import io.mockk.mockk
import io.mockk.verify
import kolbasa.AbstractPostgresqlTest
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.schema.ShardSchema
import kolbasa.pg.DatabaseExtensions.useStatement
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class PrepareKtTest : AbstractPostgresqlTest() {

    @Test
    fun testPrepare() {
        // PREPARE everything
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList())
        val shards = ShardSchema.readShards(dataSource)

        val sourceNode = nodes.keys.random()
        val targetNode = nodes.keys.filterNot { it == sourceNode }.random()

        val shardsToMove = shards
            .filterValues { it.producerNode == sourceNode.serverId }
            .keys
            .shuffled()
            .take(Random.nextInt(2, 10))  // choose a few shards to move
            .sorted()

        val migrateEvents = mockk<MigrateEvents>(relaxed = true)

        // RUN
        prepare(
            shards = shardsToMove,
            targetNode = targetNode.serverId,
            dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema),
            events = migrateEvents
        )

        // CHECK
        // first, check the table content
        dataSource.useStatement { statement ->
            val sql = """
                select
                    ${ShardSchema.PRODUCER_NODE_COLUMN_NAME}, ${ShardSchema.CONSUMER_NODE_COLUMN_NAME}, ${ShardSchema.NEXT_CONSUMER_NODE_COLUMN_NAME}
                from ${ShardSchema.SHARD_TABLE_NAME}
                where ${ShardSchema.SHARD_COLUMN_NAME} in (${shardsToMove.joinToString(",")})
            """.trimIndent()
            statement.executeQuery(sql).use { resultSet ->
                while (resultSet.next()) {
                    val producerNode = resultSet.getString(1)
                    val consumerNode = resultSet.getString(2)
                    val nextConsumerNode = resultSet.getString(3)

                    assertEquals(targetNode.serverId, producerNode)
                    assertNull(consumerNode)
                    assertEquals(targetNode.serverId, nextConsumerNode)
                }
            }
        }

        // second, check that the event was reported
        val diffs = shardsToMove.map { shardNumber ->
            val originalShard = requireNotNull(shards[shardNumber])
            val updatedShard = originalShard.copy(
                producerNode = targetNode.serverId,
                consumerNode = null,
                nextConsumerNode = targetNode.serverId
            )
            ShardDiff(originalShard, updatedShard)
        }
        verify {
            migrateEvents.prepareSuccessful(shardsToMove, targetNode.serverId, diffs)
        }
    }
}
