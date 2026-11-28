package kolbasa.cluster.admin

import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import javax.sql.DataSource
import kotlin.random.Random

class PrepareKtTest : AbstractPostgreSQLTest() {

    @Test
    fun testPrepare_Success() {
        // PREPARE everything
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        createOrRepairShardMap(nodesById(dataSources))
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
        prepare(nodesById(dataSources), targetNode.id, shardsToMove, ProgressCallback.Ignore)

        // CHECK
        // first, check the table content
        dataSource.useStatement { statement ->
            val sql = """
                select
                    ${ShardSchema.PRODUCER_NODE_COLUMN_NAME}, ${ShardSchema.CONSUMER_NODE_COLUMN_NAME}, ${ShardSchema.NEXT_CONSUMER_NODE_COLUMN_NAME}
                from ${ShardSchema.SHARD_TABLE_NAME}
                where ${ShardSchema.SHARD_COLUMN_NAME} in (${shardsToMove.joinToString(",") { it.id.toString() }})
            """.trimIndent()
            statement.executeQuery(sql).use { resultSet ->
                var checkedShards = 0
                while (resultSet.next()) {
                    val producerNode = resultSet.getString(1)
                    val consumerNode = resultSet.getString(2)
                    val nextConsumerNode = resultSet.getString(3)

                    assertEquals(targetNode.id.id, producerNode)
                    assertNull(consumerNode)
                    assertEquals(targetNode.id.id, nextConsumerNode)
                    checkedShards++
                }

                // The assertions above live inside the loop, so without this check an empty
                // result set would make the test pass having verified nothing
                assertEquals(shardsToMove.size, checkedShards)
            }
        }

        // second, check that the output was printed (ProgressCallback.Ignore is used directly now)
        // The old mockk verification is no longer applicable since prepare() uses ProgressCallback.Ignore directly.
        // We verify correctness via the database assertions above.
    }

    @Test
    fun testPrepare_TargetNodeDoesNotExist() {
        // PREPARE everything
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        createOrRepairShardMap(nodesById(dataSources))

        val targetNode = NodeId("this-node-identifier-does-not-exist-${System.nanoTime()}")

        // Which shards they are does not matter: prepare() rejects the unknown target node first
        val shardsToMove = List(3) { ShardId.random() }

        // RUN
        val exception = assertThrows<ClusterOperationException.MoveToNonExistingNodeException> {
            prepare(nodesById(dataSources), targetNode, shardsToMove, ProgressCallback.Ignore)
        }

        // CHECK
        assertInstanceOf<ClusterOperationException.MoveToNonExistingNodeException>(exception)
        assertEquals(nodes.keys.map { it.id }, exception.knownNodes)
        assertEquals(targetNode, exception.targetNode)
    }

    @Test
    fun testPrepare_MoveShardToTheSameNode() {
        // PREPARE everything
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        createOrRepairShardMap(nodesById(dataSources))
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
        val exception = assertThrows<ClusterOperationException.MoveToTheSameShardException> {
            prepare(nodesById(dataSources), targetNode.id, shardsToMove, ProgressCallback.Ignore)
        }

        // CHECK
        assertInstanceOf<ClusterOperationException.MoveToTheSameShardException>(exception)
        assertEquals(shardsToMove.first(), exception.shard)
        assertEquals(targetNode.id, exception.targetNode)
    }

    @Test
    fun testPrepareAndFinalize_WriteTheirRowsToEveryNode_AndNothingElse() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, second, third) = nodes.values.toList()
        val targetNode = nodes.keys.last().id
        createOrRepairShardMap(nodesById(dataSources))

        // The copies differ: an edit with a revision far ahead of every clock that only the first node has, a shard that the
        // second node lacks, and a row of the third node with the same revision and other nodes
        val newestRevision = Instant.parse("2100-01-01T00:00:00Z")
        moveByHand(first, shard = 500, node = nodes.keys.first().id, revision = "2100-01-01 00:00:00+00")
        deleteShard(second, 501)
        deleteShard(third, 502)
        ShardSchema.writeShards(third, listOf(Shard(ShardId.of(502), NodeId("node-x"), NodeId("node-x"), null, Instant.EPOCH)))
        val before = dataSources.associateWith { ShardSchema.readShards(it) }

        val shardsToMove = ShardSchema.readShards(first).values
            .filter { it.consumerNode != targetNode && it.shard.id < 500 }
            .map { it.shard }
            .sorted()
            .take(10)

        // PREPARE, one of the shards twice
        prepare(nodesById(dataSources), targetNode, shardsToMove + shardsToMove.first(), ProgressCallback.Ignore)
        dataSources.forEach { dataSource ->
            val prepared = ShardSchema.readShards(dataSource)
            shardsToMove.forEach { shard ->
                val expected = Shard(shard, targetNode, null, targetNode, newestRevision.plusNanos(1000))
                assertEquals(expected, prepared[shard])
            }
            assertEquals(before.getValue(dataSource) - shardsToMove.toSet(), prepared - shardsToMove.toSet())
        }

        // FINALIZE
        finalize(nodesById(dataSources), ProgressCallback.Ignore)
        dataSources.forEach { dataSource ->
            val finalized = ShardSchema.readShards(dataSource)
            shardsToMove.forEach { shard ->
                val expected = Shard(shard, targetNode, targetNode, null, newestRevision.plusNanos(2000))
                assertEquals(expected, finalized[shard])
            }
            assertEquals(before.getValue(dataSource) - shardsToMove.toSet(), finalized - shardsToMove.toSet())
        }
    }

    @Test
    fun testPrepare_HandEditOnAnotherNode_IsSeen() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, _, third) = nodes.values.toList()
        val targetNode = nodes.keys.last().id
        Cluster(dataSources, emptyList()).updateTopology()

        // A shard of another node is given to the target by hand, on one node only
        val shard = requireNotNull(ShardSchema.readShards(first).values.find { it.consumerNode != targetNode }).shard
        moveByHand(third, shard = shard.id, node = targetNode)

        // The first node doesn't have the edit, but butcher knows that the shard is on the target already
        val exception = assertThrows<ClusterOperationException.MoveToTheSameShardException> {
            prepare(nodesById(dataSources), targetNode, listOf(shard), ProgressCallback.Ignore)
        }
        assertEquals(shard, exception.shard)
    }

    @Test
    fun testPrepare_NodeRejectsRows_OtherNodesGetThem() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, middle, last) = nodes.values.toList()
        val middleNode = nodes.keys.toList()[1]
        val targetNode = nodes.keys.last().id
        Cluster(dataSources, emptyList()).updateTopology()

        // The middle node can't take any row
        rejectRows(middle)

        val shard = requireNotNull(ShardSchema.readShards(first).values.find { it.consumerNode != targetNode }).shard
        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            prepare(nodesById(dataSources), targetNode, listOf(shard), ProgressCallback.Ignore)
        }
        assertTrue("on node ${middleNode.id.id}" in exception.message, exception.message)
        assertTrue("The node takes no rows" in exception.message, exception.message)
        assertTrue("it is in effect" in exception.message, exception.message)
        assertTrue("repair the shard map" in exception.message, exception.message)

        // The nodes before and after the failed one have the change
        val prepared = requireNotNull(ShardSchema.readShards(first)[shard])
        assertEquals(targetNode, prepared.nextConsumerNode)
        assertEquals(prepared, ShardSchema.readShards(last)[shard])
    }

    @Test
    fun testPrepare_NoNodeTakesRows_NothingChanged() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val targetNode = nodes.keys.last().id
        createOrRepairShardMap(nodesById(dataSources))
        val map = ShardSchema.readShards(dataSource)
        dataSources.forEach { rejectRows(it) }

        val shard = requireNotNull(map.values.find { it.consumerNode != targetNode }).shard
        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            prepare(nodesById(dataSources), targetNode, listOf(shard), ProgressCallback.Ignore)
        }

        // Every failed node is named
        nodes.keys.forEach { node ->
            assertTrue("on node ${node.id.id}" in exception.message, exception.message)
        }
        assertTrue("nothing has changed" in exception.message, exception.message)
        dataSources.forEach { assertEquals(map, ShardSchema.readShards(it)) }
    }

    // A node that can't take any row of the shard table
    private fun rejectRows(dataSource: DataSource) {
        dataSource.useStatement { statement ->
            statement.execute(
                "create function reject_rows() returns trigger language plpgsql as " +
                    "'begin raise exception ''The node takes no rows''; end'"
            )
            statement.execute(
                "create trigger reject_rows before insert or update on ${ShardSchema.SHARD_TABLE_NAME} " +
                    "for each row execute procedure reject_rows()"
            )
        }
    }

    private fun deleteShard(dataSource: DataSource, shard: Int) {
        dataSource.useStatement { statement ->
            statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} = $shard")
        }
    }

    // Gives the shard to another node by hand, with a revision of its own if one is given
    private fun moveByHand(dataSource: DataSource, shard: Int, node: NodeId, revision: String? = null) {
        val setRevision = if (revision != null) ", ${ShardSchema.REVISION_COLUMN_NAME} = '$revision'" else ""
        dataSource.useStatement { statement ->
            statement.execute(
                "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = '${node.id}', " +
                    "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = '${node.id}'$setRevision " +
                    "where ${ShardSchema.SHARD_COLUMN_NAME} = $shard"
            )
        }
    }
}
