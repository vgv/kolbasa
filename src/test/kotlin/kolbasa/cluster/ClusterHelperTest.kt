package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.schema.IdSchema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ClusterHelperTest : AbstractPostgresqlTest() {

    @Test
    fun testReadNodes_IfSuccess() {
        val nodes = ClusterHelper
            .readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
            .keys
            .toList()

        val expected = sortedSetOf(
            requireNotNull(IdSchema.readNodeInfo(dataSource)),  // first
            requireNotNull(IdSchema.readNodeInfo(dataSourceFirstSchema)), // second
            requireNotNull(IdSchema.readNodeInfo(dataSourceSecondSchema)) // third
        ).toList()

        // Check that all nodes are read correctly
        assertEquals(expected.size, nodes.size)
        expected.forEachIndexed { index, expectedNode ->
            assertEquals(expectedNode, nodes[index])
        }
    }

    @Test
    fun testReadNodes_If_Duplicate_Server_Id() {
        // Check that all nodes have unique serverIds
        ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))

        // Make one of the nodes have the same serverId as another node
        val first = requireNotNull(IdSchema.readNodeInfo(dataSource))
        val stringId: String = first.id.id
        dataSourceFirstSchema.useStatement() { statement ->
            statement.executeUpdate("update ${IdSchema.NODE_TABLE_NAME} set ${IdSchema.ID_COLUMN_NAME} = '$stringId'")
        }

        // Check again, expect an exception
        assertThrows<IllegalStateException> {
            ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        }
    }
}
