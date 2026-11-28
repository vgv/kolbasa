package kolbasa.cluster.admin

import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.DlqOptions
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
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

class InitNodesKtTest : AbstractPostgreSQLTest() {

    @Test
    fun testInitNodes_Sets_Up_New_Nodes() {
        val admin = ClusterAdmin(mapOf(NODE_01 to dataSource, NODE_02 to dataSourceFirstSchema))

        assertEquals(listOf(NODE_01, NODE_02), admin.initNodes())
        assertNodeInfo(dataSource, NODE_01, Node.MIN_BUCKET)
        assertNodeInfo(dataSourceFirstSchema, NODE_02, Node.MIN_BUCKET + 1)

        // Every node has its id now, so there is nothing left to do
        assertTrue(admin.planInitNodes().isEmpty)
        assertEquals(emptyList<NodeId>(), admin.initNodes())
    }

    @Test
    fun testInitNodes_Matching_Node_Is_Left_Alone() {
        IdSchema.createAndInitIdTable(dataSource, NODE_01, identifierBucket = 5)

        val setUp = ClusterAdmin(mapOf(NODE_01 to dataSource)).initNodes()

        assertEquals(emptyList<NodeId>(), setUp)
        assertNodeInfo(dataSource, NODE_01, 5)
    }

    @Test
    fun testInitNodes_New_Node_Gets_Lowest_Free_Bucket() {
        IdSchema.createAndInitIdTable(dataSource, NODE_01, identifierBucket = Node.MIN_BUCKET)

        ClusterAdmin(mapOf(NODE_01 to dataSource, NODE_02 to dataSourceFirstSchema)).initNodes()

        assertNodeInfo(dataSourceFirstSchema, NODE_02, Node.MIN_BUCKET + 1)
    }

    @Test
    fun testInitNodes_Mismatch_Writes_Nothing() {
        IdSchema.createAndInitIdTable(dataSource, NODE_01, identifierBucket = Node.MIN_BUCKET)

        val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> {
            ClusterAdmin(mapOf(WRONG_ID to dataSource, NODE_02 to dataSourceFirstSchema)).initNodes()
        }

        assertEquals(mapOf(WRONG_ID to NODE_01), exception.mismatches)
        // The list is wrong, so the new node is not set up either
        assertFalse(IdSchema.nodeTableExists(dataSourceFirstSchema))
    }

    @Test
    fun testInitNodes_Empty_Node_Table() {
        IdSchema.createAndInitIdTable(dataSource, NODE_01, identifierBucket = Node.MIN_BUCKET)
        dataSource.useStatement { statement -> statement.execute("delete from ${IdSchema.NODE_TABLE_NAME}") }

        val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> {
            ClusterAdmin(mapOf(NODE_01 to dataSource, NODE_02 to dataSourceFirstSchema)).initNodes()
        }

        assertEquals(listOf(NODE_01), exception.nodesWithEmptyTable)
        // The broken node stops the new one as well
        assertFalse(IdSchema.nodeTableExists(dataSourceFirstSchema))
    }

    @Test
    fun testInitNodes_Two_Ids_Same_Database() {
        val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> {
            ClusterAdmin(mapOf(NODE_01 to dataSource, NODE_02 to dataSource)).initNodes()
        }

        assertEquals(mapOf(NODE_02 to NODE_01), exception.mismatches)
    }

    @Test
    fun testPlanInitNodes_Is_What_InitNodes_Does() {
        IdSchema.createAndInitIdTable(dataSource, NODE_01, identifierBucket = Node.MIN_BUCKET + 1)
        val admin = ClusterAdmin(
            mapOf(NODE_01 to dataSource, NODE_02 to dataSourceFirstSchema, NODE_03 to dataSourceSecondSchema)
        )

        val plan = admin.planInitNodes()

        val expected = listOf(
            NewNode(NODE_02, Node.MIN_BUCKET, hasMessages = false),
            NewNode(NODE_03, Node.MIN_BUCKET + 2, hasMessages = false)
        )
        assertEquals(expected, plan.newNodes)
        assertEquals(listOf(NODE_01), plan.initializedNodes)
        assertEquals(emptyList<NodeId>(), plan.unsafeNodes)
        assertFalse(plan.isEmpty)
        // The plan changes nothing
        assertFalse(IdSchema.nodeTableExists(dataSourceFirstSchema))
        assertFalse(IdSchema.nodeTableExists(dataSourceSecondSchema))

        assertEquals(listOf(NODE_02, NODE_03), admin.initNodes())
        assertNodeInfo(dataSourceFirstSchema, NODE_02, Node.MIN_BUCKET)
        assertNodeInfo(dataSourceSecondSchema, NODE_03, Node.MIN_BUCKET + 2)
    }

    @Test
    fun testPlanInitNodes_Node_With_Messages() {
        // A database used without a cluster: its dead letter queue has a message, and its q__node is gone
        SchemaHelpers.createOrUpdateQueues(dataSourceFirstSchema, QUEUE)
        DatabaseProducer(dataSourceFirstSchema).send(requireNotNull(QUEUE.deadLetterQueue), "dead").throwExceptionIfAny()
        dropNodeTable(dataSourceFirstSchema)

        // Alone, it becomes a cluster of one node, and that is safe
        val alone = ClusterAdmin(mapOf(NODE_02 to dataSourceFirstSchema)).planInitNodes()
        assertEquals(listOf(NewNode(NODE_02, Node.MIN_BUCKET, hasMessages = true)), alone.newNodes)
        assertEquals(emptyList<NodeId>(), alone.unsafeNodes)

        // Together with another node it is not
        val together = ClusterAdmin(mapOf(NODE_01 to dataSource, NODE_02 to dataSourceFirstSchema)).planInitNodes()
        assertEquals(listOf(false, true), together.newNodes.map { node -> node.hasMessages })
        assertEquals(listOf(NODE_02), together.unsafeNodes)
    }

    @Test
    fun testPlanInitNodes_Only_Queue_Tables_Hold_Messages() {
        // Queue tables without a message, and a table that has a row and only the name of a queue table
        SchemaHelpers.createOrUpdateQueues(dataSourceFirstSchema, QUEUE)
        dropNodeTable(dataSourceFirstSchema)
        dataSourceFirstSchema.useStatement { statement ->
            statement.execute("create table q_orders(id bigint)")
            statement.execute("insert into q_orders(id) values (1)")
        }

        val plan = ClusterAdmin(mapOf(NODE_01 to dataSource, NODE_02 to dataSourceFirstSchema)).planInitNodes()

        assertEquals(listOf(false, false), plan.newNodes.map { node -> node.hasMessages })
        assertEquals(emptyList<NodeId>(), plan.unsafeNodes)
    }

    @Test
    fun testPlanInitNodes_Refuses_A_Wrong_Id() {
        IdSchema.createAndInitIdTable(dataSource, NODE_01, identifierBucket = Node.MIN_BUCKET)

        val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> {
            ClusterAdmin(mapOf(WRONG_ID to dataSource, NODE_02 to dataSourceFirstSchema)).planInitNodes()
        }

        assertEquals(mapOf(WRONG_ID to NODE_01), exception.mismatches)
    }

    // A standalone application creates q__node along with its queues
    private fun dropNodeTable(dataSource: DataSource) {
        dataSource.useStatement { statement -> statement.execute("drop table ${IdSchema.NODE_TABLE_NAME}") }
    }

    // Both the id and the bucket: Node.equals compares the id only
    private fun assertNodeInfo(dataSource: DataSource, id: NodeId, bucket: Int) {
        val node = IdSchema.readNodeInfo(dataSource)
        assertEquals(id, node.id)
        assertEquals(bucket, node.identifiersBucket)
    }

    private companion object {
        val NODE_01 = NodeId("node-01")
        val NODE_02 = NodeId("node-02")
        val NODE_03 = NodeId("node-03")
        val WRONG_ID = NodeId("node-99")

        val QUEUE = Queue(
            name = "init_nodes_test",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(dlqOptions = DlqOptions.DEFAULT)
        )
    }
}
