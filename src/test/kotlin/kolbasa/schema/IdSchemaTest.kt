package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
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
    fun testReadNodeInfo_NullWithoutInit() {
        assertNull(IdSchema.readNodeInfo(dataSource))
    }

    @Test
    fun testUpdateIdentifiersBucketValue_Success() {
        IdSchema.createAndInitIdTable(dataSource)

        // Initial identifiers bucket state is null
        val id = IdSchema.readNodeInfo(dataSource)
        assertNotNull(id)
        assertTrue(id.identifiersBucket in Node.MIN_BUCKET .. Node.MAX_BUCKET, "$id")

        var newBucketValue = Node.randomBucket()
        while (newBucketValue == id.identifiersBucket) {
            // find different value to test update
            newBucketValue = Node.randomBucket()
        }

        val updated = IdSchema.updateIdentifiersBucket(
            dataSource,
            oldBucket = id.identifiersBucket,
            newBucket = newBucketValue
        )
        assertTrue(updated, "Update failed, WTF?")

        // Check
        val again = IdSchema.readNodeInfo(dataSource)
        assertNotNull(again)
        assertEquals(id.id, again.id)
        assertEquals(newBucketValue, again.identifiersBucket)
    }

    @Test
    fun testUpdateIdentifiersBucketValue_Failed() {
        IdSchema.createAndInitIdTable(dataSource)

        // Initial identifiers bucket state is null
        val id = IdSchema.readNodeInfo(dataSource)
        assertNotNull(id)
        assertTrue(id.identifiersBucket in Node.MIN_BUCKET .. Node.MAX_BUCKET, "$id")

        var oldBucketValue = Node.randomBucket()
        while (oldBucketValue == id.identifiersBucket) {
            // find different value to test update
            oldBucketValue = Node.randomBucket()
        }
        val newBucketValue = Node.randomBucket()

        val updated = IdSchema.updateIdentifiersBucket(
            dataSource,
            oldBucket = oldBucketValue,
            newBucket = newBucketValue
        )
        assertFalse(updated, "Update happened, WTF?")
    }

}
