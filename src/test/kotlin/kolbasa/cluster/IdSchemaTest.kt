package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import kolbasa.cluster.schema.IdSchema
import kotlin.random.Random
import kotlin.test.*

class IdSchemaTest : AbstractPostgresqlTest() {

    @Test
    fun testInitNodeTable_Check_Existing_ID_Not_Change() {
        IdSchema.createAndInitIdTable(dataSource)
        val id = IdSchema.readNodeInfo(dataSource)

        IdSchema.createAndInitIdTable(dataSource)
        val nextId = IdSchema.readNodeInfo(dataSource)

        assertEquals(id, nextId)
    }

    @Test
    fun testInitNodeTable_Check_Generated_IDs_Are_Different() {
        IdSchema.createAndInitIdTable(dataSource)
        IdSchema.createAndInitIdTable(dataSourceFirstSchema)
        IdSchema.createAndInitIdTable(dataSourceSecondSchema)

        val nodeId = IdSchema.readNodeInfo(dataSource)
        val nodeIdFirst = IdSchema.readNodeInfo(dataSourceFirstSchema)
        val nodeIdSecond = IdSchema.readNodeInfo(dataSourceSecondSchema)

        assertNotEquals(nodeId, nodeIdFirst)
        assertNotEquals(nodeId, nodeIdSecond)
    }

    @Test
    fun testReadNodeInfo_ErrorWithoutInit() {
        assertFails { IdSchema.readNodeInfo(dataSource) }
    }

    @Test
    fun testUpdateIdentifiersBucketValue() {
        IdSchema.createAndInitIdTable(dataSource)

        // Initial identifiers bucket state is null
        val id = IdSchema.readNodeInfo(dataSource)
        assertNull(id.identifierBucket)

        // Update
        val bucketValue = Random.nextInt(0, 100)
        IdSchema.updateIdentifiersBucket(dataSource, bucketValue)

        // Check
        val again = IdSchema.readNodeInfo(dataSource)
        assertEquals(id.serverId, again.serverId)
        assertEquals(bucketValue, again.identifierBucket)
    }

}
