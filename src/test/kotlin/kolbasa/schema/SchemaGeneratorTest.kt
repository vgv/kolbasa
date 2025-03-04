package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.queue.*
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SchemaGeneratorTest: AbstractPostgresqlTest() {

    data class TestMeta(
        @Searchable
        val first: Int,

        @Searchable
        @Unique
        val second: Long,

        val third: String
    )

    private val queue = Queue("test_queue", PredefinedDataTypes.String, metadata = TestMeta::class.java)

    @Test
    fun testExtractSchema_CheckStatementsAreEqualIfNoTablesAtAll() {
        val schema = SchemaGenerator.generateTableSchema(queue, null, IdRange.LOCAL_RANGE)

        assertEquals(schema.all, schema.required)
    }

    @Disabled("Disable during schema migration")
    @Test
    fun testExtractSchema_CheckRequiredStatementsAreEmptyIfSchemaIsActual() {
        // update database schema
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)

        // extract schema again
        val existingTable = SchemaExtractor.extractRawSchema(dataSource, setOf(queue.dbTableName))[queue.dbTableName]
        assertNotNull(existingTable)

        // we don't expect anything in "required", because schema is actual
        val schema = SchemaGenerator.generateTableSchema(queue, existingTable, IdRange.LOCAL_RANGE)
        assertTrue(schema.required.isEmpty(), "Required object: ${schema.required}")
    }

}
