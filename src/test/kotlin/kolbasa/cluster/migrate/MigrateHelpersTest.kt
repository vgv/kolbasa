package kolbasa.cluster.migrate

import kolbasa.AbstractPostgresqlTest
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.IllegalStateException

class MigrateHelpersTest : AbstractPostgresqlTest() {

    @Test
    fun testReadShards_Success() {
        // Create shard table on the second node
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        ShardSchema.createShardTable(dataSourceFirstSchema)
        ShardSchema.fillShardTable(dataSourceFirstSchema, nodes.keys.toList())
        val expectedShardTable = ShardSchema.readShards(dataSourceFirstSchema)

        // Check that we've found exactly the same shard table
        val shardInfo = MigrateHelpers.readShards(nodes)
        assertEquals(expectedShardTable, shardInfo.shards)
        assertEquals(dataSourceFirstSchema, shardInfo.shardDataSource)
    }

    @Test
    fun testReadShards_Failure() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))

        // Without shard table we expect an exception
        assertThrows<IllegalStateException> {
            MigrateHelpers.readShards(nodes)
        }
    }

    @Test
    fun testSplitNodes_Success() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        // Choose any node as a target node
        val targetNode = nodes.keys.random()
        val dataSourcesWithoutTargetNode = nodes
            .filter { (node, _) -> node != targetNode }
            .values
            .toList()

        val splitResult = MigrateHelpers.splitNodes(nodes, targetNode.id)
        assertEquals(dataSourcesWithoutTargetNode, splitResult.sourceNodes)
        assertEquals(nodes[targetNode], splitResult.targetNode)
    }

    @Test
    fun testSplitNodes_Failure() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))

        assertThrows<IllegalStateException> {
            MigrateHelpers.splitNodes(nodes, NodeId("bugaga-non-existing-target-id-${System.nanoTime()}"))
        }
    }

    @Test
    fun testCalculateShardsDiff_SameShards() {
        val shards = mapOf(
            1 to Shard(1, NodeId("node1"), NodeId("node1"), null),
            2 to Shard(2, NodeId("node2"), NodeId("node2"), null),
            3 to Shard(3, NodeId("node3"), NodeId("node3"), null),
        )

        val updatedShards = buildMap { putAll(shards) } // to create the copy of the map

        val diff = MigrateHelpers.calculateShardsDiff(shards, updatedShards)
        assertTrue(diff.isEmpty(), "Diff is not empty: $diff")
    }

    @Test
    fun testCalculateShardsDiff_DifferentShards() {
        val shards = mapOf(
            1 to Shard(1, NodeId("node1"), NodeId("node1"), null),
            2 to Shard(2, NodeId("node2"), NodeId("node2"), null),
            3 to Shard(3, NodeId("node3"), NodeId("node3"), null),
            4 to Shard(4, NodeId("node4"), NodeId("node4"), null),
            5 to Shard(5, NodeId("node5"), NodeId("node5"), null),
        )

        val randomShardToChange = shards.keys.random()
        val updatedShards = shards.mapValues { (shardNumber, shard) ->
            if (shardNumber == randomShardToChange) {
                shard.copy(
                    producerNode = NodeId("another_node_$shardNumber"),
                    consumerNode = null,
                    nextConsumerNode = NodeId("another_node_$shardNumber"),
                )
            } else {
                shard
            }
        }
        val originalShard = shards[randomShardToChange]
        val updatedShard = updatedShards[randomShardToChange]


        val diff = MigrateHelpers.calculateShardsDiff(shards, updatedShards)
        assertEquals(1, diff.size)
        assertEquals(originalShard, diff.first().originalShard)
        assertEquals(updatedShard, diff.first().updatedShard)
    }

}
