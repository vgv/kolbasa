package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.queue.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SchemaGeneratorTest: AbstractPostgresqlTest() {

    private val queue = Queue("test_queue", PredefinedDataTypes.String, metadata = TestMeta::class.java)

    @Test
    fun testExtractSchema_CheckStatementsAreEqualIfNoTablesAtAll() {
        val schema = SchemaGenerator.generateTableSchema(queue, null)

        assertEquals(schema.all, schema.required)
    }

    @Test
    fun testExtractSchema_CheckRequiredStatementsAreEmptyIfSchemaIsActual() {
        // update database schema
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)

        // extract schema again
        val existingTable = SchemaExtractor.extractRawSchema(dataSource, queue.dbTableName)[queue.dbTableName]
        assertNotNull(existingTable)

        // we don't expect anything in "required", because schema is actual
        val schema = SchemaGenerator.generateTableSchema(queue, existingTable)
        assertTrue(schema.required.isEmpty(), "Required object: ${schema.required}")
    }

}

private data class TestMeta(
    @Searchable
    val first: Int,

    @Searchable
    @Unique
    val second: Long,

    val third: String
)

