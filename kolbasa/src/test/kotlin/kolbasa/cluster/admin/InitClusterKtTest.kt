package kolbasa.cluster.admin

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import javax.sql.DataSource

class InitClusterKtTest : AbstractPostgreSQLTest() {

    @Test
    fun testInitCluster_NewCluster() {
        val admin = ClusterAdmin(threeNodes())

        val plan = admin.planInitCluster()

        assertEquals(listOf(NODE_01, NODE_02, NODE_03), plan.nodes.newNodes.map { node -> node.id })
        // The shards are planned for the ids the nodes get: the nodes take turns
        assertEquals(ShardId.SHARD_COUNT, plan.shards.owners.size)
        assertEquals(listOf(NODE_01, NODE_02, NODE_03), (0..2).map { plan.shards.owners.getValue(ShardId.of(it)) })
        // The new rows go to every copy, so there is nothing to sync
        assertEquals(0, plan.copiesToSync)
        assertEquals(emptyList<NodeId>(), plan.unsafeNodes)
        assertFalse(plan.isEmpty)
        // The plan changes nothing
        dataSources().forEach { node ->
            assertFalse(IdSchema.nodeTableExists(node))
            assertFalse(ShardSchema.shardTableExists(node))
        }

        val result = admin.initCluster()

        assertEquals(listOf(NODE_01, NODE_02, NODE_03), result.initializedNodes)
        assertEquals(plan.shards.owners, result.owners)
        assertEquals(mapOf(NODE_01 to 0, NODE_02 to 0, NODE_03 to 0), result.changedRows)
        assertTrue(admin.checkShardMap().isClean, "${admin.checkShardMap()}")

        // Nothing is left to do
        assertTrue(admin.planInitCluster().isEmpty)
        val again = admin.initCluster()
        assertEquals(emptyList<NodeId>(), again.initializedNodes)
        assertEquals(emptyMap<ShardId, NodeId>(), again.owners)
        assertEquals(mapOf(NODE_01 to 0, NODE_02 to 0, NODE_03 to 0), again.changedRows)
    }

    @Test
    fun testInitCluster_NewNodeJoins() {
        ClusterAdmin(threeNodes() - NODE_03).initCluster()
        val admin = ClusterAdmin(threeNodes())

        val plan = admin.planInitCluster()

        // The new node gets an id and a copy of the map. Every shard has a node already
        assertEquals(listOf(NewNode(NODE_03, Node.MIN_BUCKET + 2, hasMessages = false)), plan.nodes.newNodes)
        assertTrue(plan.shards.isEmpty)
        assertEquals(1, plan.copiesToSync)

        val result = admin.initCluster()

        assertEquals(listOf(NODE_03), result.initializedNodes)
        assertEquals(emptyMap<ShardId, NodeId>(), result.owners)
        assertEquals(mapOf(NODE_01 to 0, NODE_02 to 0, NODE_03 to ShardId.SHARD_COUNT), result.changedRows)
        assertTrue(admin.checkShardMap().isClean, "${admin.checkShardMap()}")
    }

    @Test
    fun testPlanInitCluster_NodeWithMessages() {
        // A database used without a cluster: a queue with a message, and no q__node
        SchemaHelpers.createOrUpdateQueues(dataSourceFirstSchema, QUEUE)
        DatabaseProducer(dataSourceFirstSchema).send(QUEUE, "message").throwExceptionIfAny()
        dataSourceFirstSchema.useStatement { statement -> statement.execute("drop table ${IdSchema.NODE_TABLE_NAME}") }

        // Alone, it becomes a cluster of one node, and that is safe
        assertEquals(emptyList<NodeId>(), ClusterAdmin(mapOf(NODE_02 to dataSourceFirstSchema)).planInitCluster().unsafeNodes)

        // Together with other nodes it is not: both the id and the shard map make it unsafe, and it is named once
        val plan = ClusterAdmin(threeNodes()).planInitCluster()
        assertEquals(listOf(NODE_02), plan.nodes.unsafeNodes)
        assertEquals(listOf(NODE_02), plan.shards.unsafeNodes)
        assertEquals(listOf(NODE_02), plan.unsafeNodes)
    }

    @Test
    fun testPlanInitCluster_RefusesAWrongId() {
        IdSchema.createAndInitIdTable(dataSource, NODE_01, identifierBucket = Node.MIN_BUCKET)

        val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> {
            ClusterAdmin(threeNodes() - NODE_01 + (WRONG_ID to dataSource)).planInitCluster()
        }

        assertEquals(mapOf(WRONG_ID to NODE_01), exception.mismatches)
    }

    // ---------- helpers ----------

    private fun threeNodes(): Map<NodeId, DataSource> =
        mapOf(NODE_01 to dataSource, NODE_02 to dataSourceFirstSchema, NODE_03 to dataSourceSecondSchema)

    private fun dataSources(): List<DataSource> = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

    private companion object {
        val NODE_01 = NodeId("node-01")
        val NODE_02 = NodeId("node-02")
        val NODE_03 = NodeId("node-03")
        val WRONG_ID = NodeId("node-99")

        val QUEUE = Queue(
            name = "init_cluster_test",
            databaseDataType = PredefinedDataTypes.String
        )
    }
}
