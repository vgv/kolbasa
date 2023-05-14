package kolbasa.schema

import kolbasa.AbstractPostgresTest
import org.junit.jupiter.api.Test
import kotlin.test.*

internal class SchemaExtractorTest : AbstractPostgresTest() {

    private val testTableName = "test_table"

    override fun generateTestData(): List<String> {
        val testTable = mutableListOf(
            """
            create table $testTableName(
                id bigint generated always as identity (cycle) primary key,
                created_at timestamp not null default clock_timestamp(),
                scheduled_at timestamp default clock_timestamp() + interval '12345 millisecond',
                attempts int not null default 42,
                producer varchar(256),
                meta_int_value int,
                meta_long_value bigint
            )
        """.trimIndent()
        )
        testTable += "create index if not exists ${testTableName}_scheduled_at on $testTableName(scheduled_at) where scheduled_at is not null"
        testTable += "create index if not exists ${testTableName}_composite on $testTableName(meta_int_value asc, meta_long_value desc)"
        testTable += "create unique index if not exists ${testTableName}_meta_int_unq on $testTableName(meta_int_value)"

        return testTable
    }

    @Test
    fun testExtractRawSchema() {
        val tables = SchemaExtractor.extractRawSchema(dataSource, "${testTableName}%")

        assertEquals(1, tables.size)

        val testTable = assertNotNull(tables[testTableName])

        // check columns
        assertEquals(7, testTable.columns.size, "Found columns: ${testTable.columns}")

        // id
        assertNotNull(testTable.findColumn("id")).let { idColumn ->
            assertEquals("int8", idColumn.type)
            assertFalse(idColumn.nullable)
        }

        // created_at
        assertNotNull(testTable.findColumn("created_at")).let { createdAtColumn ->
            assertEquals("timestamp", createdAtColumn.type)
            assertFalse(createdAtColumn.nullable)
            assertNotNull(createdAtColumn.defaultExpression)
        }

        // scheduled_at
        assertNotNull(testTable.findColumn("scheduled_at")).let { scheduledAtColumn ->
            assertEquals("timestamp", scheduledAtColumn.type)
            assertTrue(scheduledAtColumn.nullable)
            assertNotNull(scheduledAtColumn.defaultExpression)
        }

        // attempts
        assertNotNull(testTable.findColumn("attempts")).let { attemptsColumn ->
            assertEquals("int4", attemptsColumn.type)
            assertFalse(attemptsColumn.nullable)
            assertEquals("42", attemptsColumn.defaultExpression)
        }

        // producer
        assertNotNull(testTable.findColumn("producer")).let { producerColumn ->
            assertEquals("varchar", producerColumn.type)
            assertTrue(producerColumn.nullable)
            assertNull(producerColumn.defaultExpression)
        }

        // Check indexes
        assertEquals(4, testTable.indexes.size)

        // scheduled_at index
        assertNotNull(testTable.findIndex("${testTableName}_scheduled_at")).let { scheduledAtIndex ->
            assertFalse(scheduledAtIndex.unique)
            assertEquals("(scheduled_at IS NOT NULL)", scheduledAtIndex.filterCondition)
            assertFalse(scheduledAtIndex.invalid)
            assertEquals(1, scheduledAtIndex.columns.size)
            val scheduledAtColumn = assertNotNull(scheduledAtIndex.columns.find { it.name == "scheduled_at" })
            assertTrue(scheduledAtColumn.asc)
        }

        // composite index
        assertNotNull(testTable.findIndex("${testTableName}_composite")).let { compositeIndex ->
            assertFalse(compositeIndex.unique)
            assertNull(compositeIndex.filterCondition)
            assertFalse(compositeIndex.invalid)
            assertEquals(2, compositeIndex.columns.size)
            val intColumn = assertNotNull(compositeIndex.columns.find { it.name == "meta_int_value" })
            assertTrue(intColumn.asc)
            val longColumn = assertNotNull(compositeIndex.columns.find { it.name == "meta_long_value" })
            assertFalse(longColumn.asc)
        }

        // _meta_int_unq index
        assertNotNull(testTable.findIndex("${testTableName}_meta_int_unq")).let { compositeIndex ->
            assertTrue(compositeIndex.unique)
            assertNull(compositeIndex.filterCondition)
            assertFalse(compositeIndex.invalid)
            assertEquals(1, compositeIndex.columns.size)
            val intColumn = assertNotNull(compositeIndex.columns.find { it.name == "meta_int_value" })
            assertTrue(intColumn.asc)
        }
    }
}
