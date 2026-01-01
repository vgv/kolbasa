package kolbasa.schema

import kolbasa.schema.Schema.Companion.plus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SchemaTest {

    @Test
    fun test_EMPTY_and_IsEmpty() {
        assertTrue(Schema.EMPTY.isEmpty())
        assertTrue(Schema(emptyList(), emptyList()).isEmpty())
    }

    @Test
    fun testPlusOperator() {
        val schema1 = Schema(
            tableStatements = listOf("create table test1 (id int);"),
            indexStatements = listOf("create index idx_test1_id on test1(id);")
        )

        val schema2 = Schema(
            tableStatements = listOf("alter table test1 add column name text;"),
            indexStatements = listOf("create index idx_test1_name on test1(name);")
        )

        val combinedSchema = schema1 + schema2

        assertEquals(combinedSchema.tableStatements.size, schema1.tableStatements.size + schema2.tableStatements.size)
        assertEquals(combinedSchema.indexStatements.size, schema1.indexStatements.size + schema2.indexStatements.size)
        assertEquals(combinedSchema.tableStatements, schema1.tableStatements + schema2.tableStatements)
        assertEquals(combinedSchema.indexStatements, schema1.indexStatements + schema2.indexStatements)
    }
}
