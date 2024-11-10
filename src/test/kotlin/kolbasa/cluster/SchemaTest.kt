package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import org.junit.jupiter.api.Test
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull

class SchemaTest : AbstractPostgresqlTest() {

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


}
