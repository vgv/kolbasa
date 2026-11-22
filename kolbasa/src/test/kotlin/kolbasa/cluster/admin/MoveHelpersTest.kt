package kolbasa.cluster.admin

import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Statement
import java.time.Instant
import kotlin.IllegalStateException

class MoveHelpersTest : AbstractPostgreSQLTest() {

    @Test
    fun testReadShards_MergesTheCopiesOfEveryNode() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        val (first, second, third) = nodes.values.toList()

        // The same map on two nodes, the third one has no table yet
        ShardSchema.createOrUpdateShardTable(first)
        ShardSchema.fillShardTable(first, nodes.keys.toList())
        ShardSchema.createOrUpdateShardTable(second)
        ShardSchema.writeShards(second, ShardSchema.readShards(first).values)

        // An operator edits the copy of the second node, and no application has spread the edit yet
        second.useStatement { statement: Statement ->
            statement.execute(
                "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-x', " +
                    "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-x' where ${ShardSchema.SHARD_COLUMN_NAME} = 17"
            )
        }

        val shardMap = MoveHelpers.readShards(nodes)

        // The second copy has the newest row of every shard
        assertEquals(ShardSchema.readShards(second), shardMap.mergedShards)
        assertEquals(NodeId("node-x"), shardMap.mergedShards[ShardId.of(17)]?.producerNode)

        // Reading doesn't repair: the first node still has its old row, the third one only got an empty table
        assertNotEquals(NodeId("node-x"), ShardSchema.readShards(first)[ShardId.of(17)]?.producerNode)
        assertTrue(ShardSchema.readShards(third).isEmpty())
    }

    @Test
    fun testReadShards_NewCluster_MapSeeded() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))

        val shards = MoveHelpers.readShards(nodes).mergedShards

        assertEquals(ShardId.SHARD_COUNT, shards.size)
        assertTrue(shards.values.all { it.revision == Instant.EPOCH })
    }

    @Test
    fun testReadShards_ReadFails() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        val (node, nodeDataSource) = nodes.entries.first { it.value == dataSourceFirstSchema }
        ShardSchema.createOrUpdateShardTable(nodeDataSource)
        ShardSchema.fillShardTable(nodeDataSource, nodes.keys.toList())
        nodeDataSource.useStatement { statement: Statement ->
            statement.execute(
                "alter table ${ShardSchema.SHARD_TABLE_NAME} rename column ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} to broken"
            )
        }

        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            MoveHelpers.readShards(nodes)
        }
        // A failed read must not look like a missing table, that one gets a new map
        assertTrue("${ShardSchema.SHARD_TABLE_NAME} on node ${node.id.id}" in exception.message, exception.message)
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

        val splitResult = MoveHelpers.splitNodes(nodes, targetNode.id)
        assertEquals(dataSourcesWithoutTargetNode, splitResult.sourceNodes)
        assertEquals(nodes[targetNode], splitResult.targetNode)
    }

    @Test
    fun testSplitNodes_Failure() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))

        assertThrows<IllegalStateException> {
            MoveHelpers.splitNodes(nodes, NodeId("bugaga-non-existing-target-id-${System.nanoTime()}"))
        }
    }

    @Test
    fun testCalculateShardsDiff_SameShards() {
        val shards = mapOf(
            ShardId.of(1) to Shard(ShardId.of(1), NodeId("node1"), NodeId("node1"), null, Instant.EPOCH),
            ShardId.of(2) to Shard(ShardId.of(2), NodeId("node2"), NodeId("node2"), null, Instant.EPOCH),
            ShardId.of(3) to Shard(ShardId.of(3), NodeId("node3"), NodeId("node3"), null, Instant.EPOCH),
        )

        val updatedShards = buildMap { putAll(shards) } // to create the copy of the map

        val diff = MoveHelpers.calculateShardsDiff(shards, updatedShards)
        assertTrue(diff.isEmpty(), "Diff is not empty: $diff")
    }

    @Test
    fun testCalculateShardsDiff_OnlyRevisionChanged() {
        val shards = mapOf(
            ShardId.of(1) to Shard(ShardId.of(1), NodeId("node1"), NodeId("node1"), null, Instant.EPOCH),
            ShardId.of(2) to Shard(ShardId.of(2), NodeId("node2"), NodeId("node2"), null, Instant.EPOCH),
        )

        // The same owners with a newer revision
        val updatedShards = shards.mapValues { (_, shard) ->
            Shard(shard.shard, shard.producerNode, shard.consumerNode, shard.nextConsumerNode, Instant.now())
        }

        val diff = MoveHelpers.calculateShardsDiff(shards, updatedShards)
        assertTrue(diff.isEmpty(), "Diff is not empty: $diff")
    }

    @Test
    fun testCalculateShardsDiff_DifferentShards() {
        val shards = mapOf(
            ShardId.of(1) to Shard(ShardId.of(1), NodeId("node1"), NodeId("node1"), null, Instant.EPOCH),
            ShardId.of(2) to Shard(ShardId.of(2), NodeId("node2"), NodeId("node2"), null, Instant.EPOCH),
            ShardId.of(3) to Shard(ShardId.of(3), NodeId("node3"), NodeId("node3"), null, Instant.EPOCH),
            ShardId.of(4) to Shard(ShardId.of(4), NodeId("node4"), NodeId("node4"), null, Instant.EPOCH),
            ShardId.of(5) to Shard(ShardId.of(5), NodeId("node5"), NodeId("node5"), null, Instant.EPOCH),
        )

        val randomShardToChange = shards.keys.random()
        val updatedShards = shards.mapValues { (shardNumber, shard) ->
            if (shardNumber == randomShardToChange) {
                Shard(
                    shard = shard.shard,
                    producerNode = NodeId("another_node_${shardNumber.id}"),
                    consumerNode = null,
                    nextConsumerNode = NodeId("another_node_${shardNumber.id}"),
                    revision = shard.revision
                )
            } else {
                shard
            }
        }
        val originalShard = shards[randomShardToChange]
        val updatedShard = updatedShards[randomShardToChange]


        val diff = MoveHelpers.calculateShardsDiff(shards, updatedShards)
        assertEquals(1, diff.size)
        assertEquals(originalShard?.toOwnership(), diff.first().originalShard)
        assertEquals(updatedShard?.toOwnership(), diff.first().updatedShard)
    }

}
