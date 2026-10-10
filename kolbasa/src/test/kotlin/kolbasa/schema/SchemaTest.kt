package kolbasa.schema

import kolbasa.schema.Schema.Companion.merge
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random

class SchemaTest {

    @Test
    fun test_IsEmpty_And_Size() {
        // Special case: empty schema
        assertTrue(Schema.EMPTY.isEmpty)
        assertEquals(0, Schema.EMPTY.size)

        assertTrue(Schema(emptyList(), emptyList()).isEmpty)
        assertEquals(0, Schema(emptyList(), emptyList()).size)

        // Random schema
        val schema = Schema(
            tableStatements = (1..Random.nextInt(20)).map { "create table test_$it(id int);" },
            indexStatements = (1..Random.nextInt(20)).map { "create index index_${it}_id on test_$it(id);" }
        )
        assertEquals(schema.tableStatements.size + schema.indexStatements.size, schema.size)
        assertFalse(schema.isEmpty)
    }

    @Test
    fun testMerge() {
        val schemas = 10
        val generatedSchemas = (1..schemas).map { index ->
            Schema(
                tableStatements = listOf("create table table_$index(id int);"),
                indexStatements = listOf("create index index_${index}_id on table_$index(id);")
            )
        }

        val combinedSchema = generatedSchemas.merge()

        assertEquals(combinedSchema.tableStatements.size, schemas)
        assertEquals(combinedSchema.indexStatements.size, schemas)
        assertEquals(combinedSchema.tableStatements, generatedSchemas.flatMap { it.tableStatements })
        assertEquals(combinedSchema.indexStatements, generatedSchemas.flatMap { it.indexStatements })
    }
}
