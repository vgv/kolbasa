package kolbasa.cluster.admin

import kolbasa.cluster.Cluster
import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant

class SyncShardsKtTest : AbstractPostgreSQLTest() {

    @Test
    fun testSyncShards_CopiesDiffer() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelpers.readOrCreateNodes(dataSources)
        val (first, second, third) = nodes.values.toList()
        val admin = ClusterAdmin(nodesById(dataSources))
        Cluster(dataSources, emptyList()).updateTopology()

        // The first node has an edit by hand. The second copy has an older row, a row with the same revision and other nodes,
        // and no shard 7. No node has shard 8. The third node has lost its table
        val shard9 = requireNotNull(ShardSchema.readShards(first)[ShardId.of(9)])
        first.useStatement { statement ->
            statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} = 8")
            statement.execute(
                "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-x', " +
                    "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-x' where ${ShardSchema.SHARD_COLUMN_NAME} = 5"
            )
        }
        second.useStatement { statement ->
            statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} in (7, 8, 9)")
        }
        ShardSchema.writeShards(second, listOf(Shard(ShardId.of(9), NodeId("node-y"), NodeId("node-y"), null, Instant.EPOCH)))
        third.useStatement { it.execute("drop table ${ShardSchema.SHARD_TABLE_NAME}") }

        val changedRows = admin.syncShards()

        assertEquals(listOf(1, 3, ShardId.SHARD_COUNT - 1), nodes.keys.map { changedRows.getValue(it.id) })
        val shards = ShardSchema.readShards(first)
        dataSources.forEach { assertEquals(shards, ShardSchema.readShards(it)) }
        assertEquals(NodeId("node-x"), shards[ShardId.of(5)]?.producerNode)
        // The row of the node with the smallest id won, with a new revision
        assertTrue(shards.getValue(ShardId.of(9)).equalsIgnoreRevision(shard9))
        assertTrue(shards.getValue(ShardId.of(9)).revision > Instant.EPOCH)
        // A shard that no node has gets no node here
        assertEquals(ShardId.SHARD_COUNT - 1, shards.size)
        assertFalse(ShardId.of(8) in shards)

        // Nothing is left to do
        assertEquals(nodes.keys.associate { it.id to 0 }, admin.syncShards())
    }

    @Test
    fun testSyncShards_NodeRejectsRows_OtherNodesSynced() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelpers.readOrCreateNodes(dataSources)
        val (_, middle, last) = nodes.values.toList()
        val middleNode = nodes.keys.toList()[1].id
        val admin = ClusterAdmin(nodesById(dataSources))
        Cluster(dataSources, emptyList()).updateTopology()

        listOf(middle, last).forEach { node ->
            node.useStatement { statement ->
                statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} < 10")
            }
        }
        middle.useStatement { statement ->
            statement.execute(
                "create function reject_rows() returns trigger language plpgsql as " +
                    "'begin raise exception ''The node takes no rows''; end'"
            )
            statement.execute(
                "create trigger reject_rows before insert or update on ${ShardSchema.SHARD_TABLE_NAME} " +
                    "for each row execute procedure reject_rows()"
            )
        }

        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            admin.syncShards()
        }

        assertTrue("on node ${middleNode.id}" in exception.message, exception.message)
        assertEquals(ShardId.SHARD_COUNT, ShardSchema.readShards(last).size)
    }

    @Test
    fun testSyncShards_NoShardMap() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val admin = ClusterAdmin(nodesById(dataSources))
        // A table without rows is no map either
        ShardSchema.createOrUpdateShardTable(dataSource)

        val exception = assertThrows<ClusterOperationException.ShardsNotInitializedException> {
            admin.syncShards()
        }

        assertTrue("Initialize the cluster first" in exception.message, exception.message)
        // Nothing is created: the other nodes have no table still
        assertEquals(0, ShardSchema.readShards(dataSource).size)
        assertFalse(ShardSchema.shardTableExists(dataSourceFirstSchema))
        assertFalse(ShardSchema.shardTableExists(dataSourceSecondSchema))
    }
}
