package kolbasa.cluster.admin

import kolbasa.AbstractPostgreSQLTest
import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.postgresql.ds.PGSimpleDataSource

class ClusterAdminHelpersTest : AbstractPostgreSQLTest() {

    @Test
    fun testCheckNodesAvailable_All_Reachable() {
        ClusterAdminHelpers.checkNodesAvailable(listOf(dataSource, dataSourceFirstSchema))
    }

    @Test
    fun testCheckNodesAvailable_Unreachable_Node() {
        val unreachable = PGSimpleDataSource().apply {
            serverNames = arrayOf("127.0.0.1")
            portNumbers = intArrayOf(9)
            databaseName = "there-is-no-such-database"
        }

        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            ClusterAdminHelpers.checkNodesAvailable(listOf(dataSource, unreachable))
        }

        assertTrue(exception.message.contains("Can't connect to 1 of 2"), exception.message)
    }

    @Test
    fun testCheckNodeIds_Sets_Up_New_Nodes() {
        val setUp = ClusterAdminHelpers.checkNodeIds(
            mapOf(NodeId("node-01") to dataSource, NodeId("node-02") to dataSourceFirstSchema)
        )

        assertEquals(listOf(NodeId("node-01"), NodeId("node-02")), setUp)
        assertEquals(Node(NodeId("node-01"), Node.MIN_BUCKET), IdSchema.readNodeInfo(dataSource))
        assertEquals(Node(NodeId("node-02"), Node.MIN_BUCKET + 1), IdSchema.readNodeInfo(dataSourceFirstSchema))
    }

    @Test
    fun testCheckNodeIds_Matching_Node_Is_Left_Alone() {
        IdSchema.createAndInitIdTable(dataSource, NodeId("node-01"), identifierBucket = 5)

        val setUp = ClusterAdminHelpers.checkNodeIds(mapOf(NodeId("node-01") to dataSource))

        assertEquals(emptyList<NodeId>(), setUp)
        assertEquals(Node(NodeId("node-01"), 5), IdSchema.readNodeInfo(dataSource))
    }

    @Test
    fun testCheckNodeIds_New_Node_Gets_Lowest_Free_Bucket() {
        IdSchema.createAndInitIdTable(dataSource, NodeId("node-01"), identifierBucket = Node.MIN_BUCKET)

        ClusterAdminHelpers.checkNodeIds(
            mapOf(NodeId("node-01") to dataSource, NodeId("node-02") to dataSourceFirstSchema)
        )

        assertEquals(Node(NodeId("node-02"), Node.MIN_BUCKET + 1), IdSchema.readNodeInfo(dataSourceFirstSchema))
    }

    @Test
    fun testCheckNodeIds_Mismatch_Writes_Nothing() {
        IdSchema.createAndInitIdTable(dataSource, NodeId("node-01"), identifierBucket = Node.MIN_BUCKET)

        val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> {
            ClusterAdminHelpers.checkNodeIds(
                mapOf(NodeId("node-99") to dataSource, NodeId("node-02") to dataSourceFirstSchema)
            )
        }

        assertEquals(mapOf(NodeId("node-99") to NodeId("node-01")), exception.mismatches)
        // The list is wrong, so the new node is not set up either
        assertFalse(IdSchema.nodeTableExists(dataSourceFirstSchema))
    }

    @Test
    fun testCheckNodeIds_Empty_Node_Table() {
        IdSchema.createAndInitIdTable(dataSource, NodeId("node-01"), identifierBucket = Node.MIN_BUCKET)
        dataSource.useStatement { statement -> statement.execute("delete from ${IdSchema.NODE_TABLE_NAME}") }

        val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> {
            ClusterAdminHelpers.checkNodeIds(mapOf(NodeId("node-01") to dataSource))
        }

        assertEquals(listOf(NodeId("node-01")), exception.nodesWithoutId)
    }

    @Test
    fun testCheckNodeIds_Two_Ids_Same_Database() {
        val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> {
            ClusterAdminHelpers.checkNodeIds(mapOf(NodeId("node-01") to dataSource, NodeId("node-02") to dataSource))
        }

        assertEquals(mapOf(NodeId("node-02") to NodeId("node-01")), exception.mismatches)
    }
}
