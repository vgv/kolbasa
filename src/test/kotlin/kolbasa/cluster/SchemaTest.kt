package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SchemaTest : AbstractPostgresqlTest() {

    @Test
    fun testCreateAndInitTable_InitialConditions() {
        Schema.createAndInitNodeTable(dataSource)

        // Check initial conditions
        assertNotNull(Schema.readNodeInfo(dataSource)).let { nodeInfo ->
            assertTrue(nodeInfo.sendEnabled, "$nodeInfo")
            assertTrue(nodeInfo.receiveEnabled, "$nodeInfo")
        }
    }

    @Test
    fun testInitNodeTable() {
        Schema.createAndInitNodeTable(dataSource)
        Schema.createAndInitNodeTable(dataSourceFirstSchema)
        Schema.createAndInitNodeTable(dataSourceSecondSchema)

        val nodeInfo = assertNotNull(Schema.readNodeInfo(dataSource))
        val nodeInfoFirst = assertNotNull(Schema.readNodeInfo(dataSourceFirstSchema))
        val nodeInfoSecond = assertNotNull(Schema.readNodeInfo(dataSourceSecondSchema))

        assertNotEquals(nodeInfo.serverId, nodeInfoFirst.serverId)
        assertNotEquals(nodeInfo.serverId, nodeInfoSecond.serverId)
    }

    @Test
    fun testUpdateNodeTable() {
        Schema.createAndInitNodeTable(dataSource)

        // Check initial conditions
        assertNotNull(Schema.readNodeInfo(dataSource)).let { nodeInfo ->
            assertTrue(nodeInfo.sendEnabled, "$nodeInfo")
            assertTrue(nodeInfo.receiveEnabled, "$nodeInfo")
        }

        // Change one field and test
        Schema.updateNodeInfo(dataSource, sendEnabled = false, receiveEnabled = true)
        assertNotNull(Schema.readNodeInfo(dataSource)).let { nodeInfo ->
            assertFalse(nodeInfo.sendEnabled, "$nodeInfo")
            assertTrue(nodeInfo.receiveEnabled, "$nodeInfo")
        }

        // Change another field and test
        Schema.updateNodeInfo(dataSource, sendEnabled = true, receiveEnabled = false)
        assertNotNull(Schema.readNodeInfo(dataSource)).let { nodeInfo ->
            assertTrue(nodeInfo.sendEnabled, "$nodeInfo")
            assertFalse(nodeInfo.receiveEnabled, "$nodeInfo")
        }
    }

}
