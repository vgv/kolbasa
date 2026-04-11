package kolbasa.cluster.butcher

import kolbasa.AbstractPostgresqlTest
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.butcher.config.ClusterNodes
import kolbasa.cluster.butcher.config.Command
import kolbasa.cluster.schema.ShardSchema
import kolbasa.utils.JdbcHelpers.useStatement
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows
import kotlin.random.Random

class PrepareKtTest : AbstractPostgresqlTest() {

    @Test
    fun testPrepare_Success() {
        // PREPARE everything
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList())
        val shards = ShardSchema.readShards(dataSource)

        val sourceNode = nodes.keys.random()
        val targetNode = nodes.keys.filterNot { it == sourceNode }.random()

        val shardsToMove = shards
            .filterValues { it.producerNode == sourceNode.id }
            .keys
            .shuffled()
            .take(Random.nextInt(2, 10))  // choose a few shards to move
            .sorted()

        // RUN
        prepare(Command.Prepare(ClusterNodes(dataSources), targetNode.id, shardsToMove))

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

                    assertEquals(targetNode.id.id, producerNode)
                    assertNull(consumerNode)
                    assertEquals(targetNode.id.id, nextConsumerNode)
                }
            }
        }

        // second, check that the output was printed (ConsoleProgressCallback is used directly now)
        // The old mockk verification is no longer applicable since prepare() uses ConsoleProgressCallback directly.
        // We verify correctness via the database assertions above.
    }

    @Test
    fun testPrepare_TargetNodeDoesNotExist() {
        // PREPARE everything
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList())

        val targetNode = NodeId("this-node-identifier-does-not-exist-${System.nanoTime()}")

        val shardsToMove = listOf(Random.nextInt(1, 100000), Random.nextInt(1, 100000), Random.nextInt(1, 100000))

        // RUN
        val exception = assertThrows<ButcherException.MoveToNonExistingNodeException> {
            prepare(Command.Prepare(ClusterNodes(dataSources), targetNode, shardsToMove))
        }

        // CHECK
        assertInstanceOf<ButcherException.MoveToNonExistingNodeException>(exception)
        assertEquals(nodes.keys.toList(), exception.knownNodes)
        assertEquals(targetNode, exception.targetNode)
    }

    @Test
    fun testPrepare_MoveShardToTheSameNode() {
        // PREPARE everything
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList())
        val shards = ShardSchema.readShards(dataSource)

        val sourceNode = nodes.keys.random()
        val targetNode = nodes.keys.filterNot { it == sourceNode }.random()

        val shardsToMove = shards
            .filterValues { it.producerNode == targetNode.id } // move shards that are already on the target node
            .keys
            .shuffled()
            .take(Random.nextInt(2, 10))  // choose a few shards to move
            .sorted()

        // RUN
        val exception = assertThrows<ButcherException.MoveToTheSameShardException> {
            prepare(Command.Prepare(ClusterNodes(dataSources), targetNode.id, shardsToMove))
        }

        // CHECK
        assertInstanceOf<ButcherException.MoveToTheSameShardException>(exception)
        assertEquals(shards[shardsToMove.first()], exception.shard)
        assertEquals(targetNode.id, exception.targetNode)
    }
}
