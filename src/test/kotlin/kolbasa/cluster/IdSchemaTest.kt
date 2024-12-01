package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import org.junit.jupiter.api.Test
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull

class IdSchemaTest : AbstractPostgresqlTest() {

    @Test
    fun testInitNodeTable_Check_Generated_IDs_Are_Different() {
        IdSchema.createAndInitIdTable(dataSource)
        IdSchema.createAndInitIdTable(dataSourceFirstSchema)
        IdSchema.createAndInitIdTable(dataSourceSecondSchema)

        val nodeId = assertNotNull(IdSchema.readNodeId(dataSource))
        val nodeIdFirst = assertNotNull(IdSchema.readNodeId(dataSourceFirstSchema))
        val nodeIdSecond = assertNotNull(IdSchema.readNodeId(dataSourceSecondSchema))

        assertNotEquals(nodeId, nodeIdFirst)
        assertNotEquals(nodeId, nodeIdSecond)
    }

}
