package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull

class SchemaGeneratorTest : AbstractPostgresqlTest() {

    private val FIRST = MetaField.int("first", FieldOption.SEARCH)
    private val SECOND = MetaField.long("second", FieldOption.ALL_LIVE_UNIQUE)
    private val THIRD = MetaField.string("third")

    private val queue = Queue.of(
        "test_queue",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIRST, SECOND, THIRD)
    )

    @Test
    fun testExtractSchema_CreateOrUpdate_Check_Statements_If_No_Tables_At_All() {
        val schema = SchemaGenerator.generateTableSchema(queue, null, IdRange.LOCAL_RANGE)

        // Table DDL
        val createTableStatements = 1
        val addInternalColumns = 1 // alter remaining_attempts
        val addMetaColumns = queue.metadata.fields.size
        assertEquals(createTableStatements + addInternalColumns + addMetaColumns, schema.tableStatements.size)

        // Indexes DDL
        val internalIndexes = 2  // shard index + scheduled_at index
        val metaIndexes = queue.metadata.fields.count { it.option != FieldOption.NONE }
        assertEquals(internalIndexes + metaIndexes, schema.indexStatements.size)
    }

    @Test
    fun testExtractSchema_CreateOrUpdate_Check_Statements_Are_Empty_If_Schema_Is_Actual() {
        // update database schema
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // extract schema again
        val existingTable = SchemaExtractor.extractRawSchema(dataSource, setOf(queue.dbTableName))[queue.dbTableName]
        assertNotNull(existingTable)

        // we don't expect anything because schema is actual
        val schema = SchemaGenerator.generateTableSchema(queue, existingTable, IdRange.LOCAL_RANGE)
        assertTrue(schema.isEmpty, "Required object: $schema")
        assertEquals(0, schema.size, "Required object: $schema")
    }

    @Test
    fun testExtractSchema_Rename_Check_Statements_If_Table_Exists() {
        // Create table
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        val existingTable = SchemaExtractor.extractRawSchema(dataSource, setOf(queue.dbTableName))[queue.dbTableName]
        val schema = SchemaGenerator.generateRenameTableSchema(queue, existingTable, "new_queue_table")

        // Table DDL
        // One rename statement
        assertEquals(1, schema.tableStatements.size)
        // Rough check that rename statement contains necessary words
        assertTrue(schema.tableStatements.first().contains("alter table"))
        assertTrue(schema.tableStatements.first().contains("new_queue_table"))
        assertTrue(schema.tableStatements.first().contains(queue.dbTableName))

        // Indexes DDL
        assertEquals(0, schema.indexStatements.size)
    }

    @Test
    fun testExtractSchema_Rename_Check_Statements_If_No_Table() {
        val schema = SchemaGenerator.generateRenameTableSchema(queue, null, "new_queue_table")

        // Schema should be empty
        assertTrue(schema.isEmpty, "Schema: $schema")
        assertEquals(0, schema.size, "Schema: $schema")
    }

    @Test
    fun testExtractSchema_Delete_Check_Statements_If_Table_Exists() {
        // Create table
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        val existingTable = SchemaExtractor.extractRawSchema(dataSource, setOf(queue.dbTableName))[queue.dbTableName]
        val schema = SchemaGenerator.generateDropTableSchema(queue, existingTable)

        // Table DDL
        // One drop statement
        assertEquals(1, schema.tableStatements.size)
        // Rough check that delete statement contains necessary words
        assertTrue(schema.tableStatements.first().contains("drop table"))
        assertTrue(schema.tableStatements.first().contains(queue.dbTableName))

        // Indexes DDL
        assertEquals(0, schema.indexStatements.size)
    }

    @Test
    fun testExtractSchema_Delete_Check_Statements_If_No_Table() {
        val schema = SchemaGenerator.generateDropTableSchema(queue, null)

        // Schema should be empty
        assertTrue(schema.isEmpty, "Schema: $schema")
        assertEquals(0, schema.size, "Schema: $schema")
    }
}
