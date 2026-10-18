package kolbasa.schema

import kolbasa.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class IdSchemaTest : AbstractPostgreSQLTest() {

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
    fun testInitNodeTable_With_Given_Id() {
        val created = IdSchema.createAndInitIdTable(dataSource, NodeId("node-01"), identifierBucket = 7)

        assertTrue(created)
        assertEquals(Node(NodeId("node-01"), 7), IdSchema.readNodeInfo(dataSource))
    }

    @Test
    fun testInitNodeTable_With_Given_Id_Keeps_Existing_Id() {
        IdSchema.createAndInitIdTable(dataSource)
        val existing = IdSchema.readNodeInfo(dataSource)

        val created = IdSchema.createAndInitIdTable(dataSource, NodeId("node-01"), identifierBucket = 7)

        assertFalse(created)
        assertEquals(existing, IdSchema.readNodeInfo(dataSource))
    }

    @Test
    fun testInitNodeTable_With_Given_Id_Is_Not_Part_Of_Sql() {
        // A quote would break the statement if the id were pasted into the SQL
        val created = IdSchema.createAndInitIdTable(dataSource, NodeId("it's"), identifierBucket = 7)

        assertTrue(created)
        assertEquals(NodeId("it's"), IdSchema.readNodeInfo(dataSource).id)
    }

    @Test
    fun testNodeTableExists() {
        assertFalse(IdSchema.nodeTableExists(dataSource))

        IdSchema.createAndInitIdTable(dataSource)

        assertTrue(IdSchema.nodeTableExists(dataSource))
    }

    @Test
    fun testFindNodeInfo_No_Active_Row() {
        IdSchema.createAndInitIdTable(dataSource)
        dataSource.useStatement { statement -> statement.execute("delete from ${IdSchema.NODE_TABLE_NAME}") }

        assertTrue(IdSchema.nodeTableExists(dataSource))
        assertNull(IdSchema.findNodeInfo(dataSource))
    }

    @Test
    fun testUpdateIdentifiersBucketValue_Success() {
        IdSchema.createAndInitIdTable(dataSource)

        // Initial identifiers bucket state is null
        val id = IdSchema.readNodeInfo(dataSource)
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
        assertEquals(id.id, again.id)
        assertEquals(newBucketValue, again.identifiersBucket)
    }

    @Test
    fun testUpdateIdentifiersBucketValue_Failed() {
        IdSchema.createAndInitIdTable(dataSource)

        // Initial identifiers bucket state is null
        val id = IdSchema.readNodeInfo(dataSource)
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
