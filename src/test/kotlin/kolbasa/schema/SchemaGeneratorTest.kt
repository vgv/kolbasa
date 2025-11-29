package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SchemaGeneratorTest : AbstractPostgresqlTest() {

    private val FIRST = MetaField.int("first", FieldOption.SEARCH)
    private val SECOND = MetaField.long("second", FieldOption.STRICT_UNIQUE)
    private val THIRD = MetaField.string("third")

    private val queue = Queue.of(
        "test_queue",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIRST, SECOND, THIRD)
    )

    @Test
    fun testExtractSchema_Check_Statements_If_No_Tables_At_All() {
        val schema = SchemaGenerator.generateTableSchema(queue, null, IdRange.LOCAL_RANGE)

        // Table DDL
        val createTableStatements = 1
        val addInternalColumns = 2 // add shard and alter remaining_attempts
        val addMetaColumns = queue.metadata.fields.size
        assertEquals(createTableStatements + addInternalColumns + addMetaColumns, schema.tableStatements.size)

        // Indexes DDL
        val internalIndexes = 2  // shard index + scheduled_at index
        val metaIndexes = queue.metadata.fields.count { it.option != FieldOption.NONE }
        assertEquals(internalIndexes + metaIndexes, schema.indexStatements.size)
    }

    @Test
    fun testExtractSchema_Check_Statements_Are_Empty_If_Schema_Is_Actual() {
        // update database schema
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)

        // extract schema again
        val existingTable = SchemaExtractor.extractRawSchema(dataSource, setOf(queue.dbTableName))[queue.dbTableName]
        assertNotNull(existingTable)

        // we don't expect anything in "required", because schema is actual
        val schema = SchemaGenerator.generateTableSchema(queue, existingTable, IdRange.LOCAL_RANGE)
        assertTrue(schema.isEmpty(), "Required object: $schema")
    }

}
