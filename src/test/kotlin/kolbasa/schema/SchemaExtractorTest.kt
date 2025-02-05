package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import org.junit.jupiter.api.Test
import kotlin.test.*

internal class SchemaExtractorTest : AbstractPostgresqlTest() {

    private val testTableName = Const.QUEUE_TABLE_NAME_PREFIX + "test_table"
    private val minValue = 123.toLong()
    private val maxValue = 4561123.toLong()
    private val cacheValue = 42.toLong()
    private val incrementValue = 34.toLong()

    override fun generateTestData(): List<String> {
        val testTable = mutableListOf(
            """
            create table $testTableName(
                id bigint generated always as identity (minvalue $minValue maxvalue $maxValue cache $cacheValue increment $incrementValue cycle) primary key,
                created_at timestamp not null default clock_timestamp(),
                scheduled_at timestamp default clock_timestamp() + interval '12345 millisecond',
                attempts int not null default 42,
                producer varchar(256),
                meta_string_value varchar(256),
                meta_long_value bigint,
                meta_int_value int,
                meta_short_value smallint,
                meta_boolean_value boolean,
                meta_double_value double precision,
                meta_float_value real,
                meta_char_value char(1),
                meta_biginteger_value numeric
            )
        """.trimIndent()
        )
        testTable += "create index if not exists ${testTableName}_scheduled_at on $testTableName(scheduled_at) where scheduled_at is not null"
        testTable += "create index if not exists ${testTableName}_composite on $testTableName(meta_int_value asc, meta_long_value desc)"
        testTable += "create unique index if not exists ${testTableName}_meta_int_unq on $testTableName(meta_int_value)"

        return testTable
    }

    // Generate the same tables/indexes in every schema
    override fun generateTestDataFirstSchema() = generateTestData()

    // Generate the same tables/indexes in every schema
    override fun generateTestDataSecondSchema() = generateTestData()

    @Test
    fun testExtractRawSchema() {
        // here we have to find objects (tables, indexes etc.) only from 'public' schema
        val tables = SchemaExtractor.extractRawSchema(dataSource, setOf(testTableName))

        assertEquals(1, tables.size, "Tables: ${tables.keys}")

        val testTable = assertNotNull(tables[testTableName])

        // check columns
        assertEquals(14, testTable.columns.size, "Found columns: ${testTable.columns}")

        // id and identity
        assertNotNull(testTable.findColumn("id")).let { idColumn ->
            assertEquals(ColumnType.BIGINT, idColumn.type)
            assertFalse(idColumn.nullable)
        }
        assertEquals(minValue, testTable.identity.min)
        assertEquals(minValue, testTable.identity.start)
        assertEquals(maxValue, testTable.identity.max)
        assertEquals(cacheValue, testTable.identity.cache)
        assertEquals(incrementValue, testTable.identity.increment)
        assertEquals(true, testTable.identity.cycles)

        // created_at
        assertNotNull(testTable.findColumn("created_at")).let { createdAtColumn ->
            assertEquals(ColumnType.TIMESTAMP, createdAtColumn.type)
            assertFalse(createdAtColumn.nullable)
            assertNotNull(createdAtColumn.defaultExpression)
        }

        // scheduled_at
        assertNotNull(testTable.findColumn("scheduled_at")).let { scheduledAtColumn ->
            assertEquals(ColumnType.TIMESTAMP, scheduledAtColumn.type)
            assertTrue(scheduledAtColumn.nullable)
            assertNotNull(scheduledAtColumn.defaultExpression)
        }

        // attempts
        assertNotNull(testTable.findColumn("attempts")).let { attemptsColumn ->
            assertEquals(ColumnType.INT, attemptsColumn.type)
            assertFalse(attemptsColumn.nullable)
            assertEquals("42", attemptsColumn.defaultExpression)
        }

        // producer
        assertNotNull(testTable.findColumn("producer")).let { producerColumn ->
            assertEquals(ColumnType.VARCHAR, producerColumn.type)
            assertTrue(producerColumn.nullable)
            assertNull(producerColumn.defaultExpression)
        }

        // meta_string_value
        assertNotNull(testTable.findColumn("meta_string_value")).let { metaStringValueColumn ->
            assertEquals(ColumnType.VARCHAR, metaStringValueColumn.type)
            assertTrue(metaStringValueColumn.nullable)
            assertNull(metaStringValueColumn.defaultExpression)
        }

        // meta_long_value
        assertNotNull(testTable.findColumn("meta_long_value")).let { metaLongValueColumn ->
            assertEquals(ColumnType.BIGINT, metaLongValueColumn.type)
            assertTrue(metaLongValueColumn.nullable)
            assertNull(metaLongValueColumn.defaultExpression)
        }

        // meta_int_value
        assertNotNull(testTable.findColumn("meta_int_value")).let { metaIntValueColumn ->
            assertEquals(ColumnType.INT, metaIntValueColumn.type)
            assertTrue(metaIntValueColumn.nullable)
            assertNull(metaIntValueColumn.defaultExpression)
        }

        // meta_short_value
        assertNotNull(testTable.findColumn("meta_short_value")).let { metaShortValueColumn ->
            assertEquals(ColumnType.SMALLINT, metaShortValueColumn.type)
            assertTrue(metaShortValueColumn.nullable)
            assertNull(metaShortValueColumn.defaultExpression)
        }

        // meta_boolean_value
        assertNotNull(testTable.findColumn("meta_boolean_value")).let { metaBooleanValueColumn ->
            assertEquals(ColumnType.BOOLEAN, metaBooleanValueColumn.type)
            assertTrue(metaBooleanValueColumn.nullable)
            assertNull(metaBooleanValueColumn.defaultExpression)
        }

        // meta_double_value
        assertNotNull(testTable.findColumn("meta_double_value")).let { metaDoubleValueColumn ->
            assertEquals(ColumnType.DOUBLE, metaDoubleValueColumn.type)
            assertTrue(metaDoubleValueColumn.nullable)
            assertNull(metaDoubleValueColumn.defaultExpression)
        }

        // meta_float_value
        assertNotNull(testTable.findColumn("meta_float_value")).let { metaFloatValueColumn ->
            assertEquals(ColumnType.REAL, metaFloatValueColumn.type)
            assertTrue(metaFloatValueColumn.nullable)
            assertNull(metaFloatValueColumn.defaultExpression)
        }

        // meta_char_value
        assertNotNull(testTable.findColumn("meta_char_value")).let { metaCharValueColumn ->
            assertEquals(ColumnType.CHAR, metaCharValueColumn.type)
            assertTrue(metaCharValueColumn.nullable)
            assertNull(metaCharValueColumn.defaultExpression)
        }

        // meta_biginteger_value
        assertNotNull(testTable.findColumn("meta_biginteger_value")).let { metaBigIntegerValueColumn ->
            assertEquals(ColumnType.NUMERIC, metaBigIntegerValueColumn.type)
            assertTrue(metaBigIntegerValueColumn.nullable)
            assertNull(metaBigIntegerValueColumn.defaultExpression)
        }

        // Check indexes
        assertEquals(4, testTable.indexes.size, "Indexes: ${testTable.indexes}")

        // scheduled_at index
        assertNotNull(testTable.findIndex("${testTableName}_scheduled_at")).let { scheduledAtIndex ->
            assertFalse(scheduledAtIndex.unique)
            assertEquals("(scheduled_at IS NOT NULL)", scheduledAtIndex.filterCondition)
            assertFalse(scheduledAtIndex.invalid)
            assertEquals(1, scheduledAtIndex.columns.size, "Columns: ${scheduledAtIndex.columns}")
            val scheduledAtColumn = assertNotNull(scheduledAtIndex.columns.find { it.name == "scheduled_at" })
            assertTrue(scheduledAtColumn.asc)
        }

        // composite index
        assertNotNull(testTable.findIndex("${testTableName}_composite")).let { compositeIndex ->
            assertFalse(compositeIndex.unique)
            assertNull(compositeIndex.filterCondition)
            assertFalse(compositeIndex.invalid)
            assertEquals(2, compositeIndex.columns.size, "Columns: ${compositeIndex.columns}")
            val intColumn = assertNotNull(compositeIndex.columns.find { it.name == "meta_int_value" })
            assertTrue(intColumn.asc)
            val longColumn = assertNotNull(compositeIndex.columns.find { it.name == "meta_long_value" })
            assertFalse(longColumn.asc)
        }

        // _meta_int_unq index
        assertNotNull(testTable.findIndex("${testTableName}_meta_int_unq")).let { metaFieldIndex ->
            assertTrue(metaFieldIndex.unique)
            assertNull(metaFieldIndex.filterCondition)
            assertFalse(metaFieldIndex.invalid)
            assertEquals(1, metaFieldIndex.columns.size, "Columns: ${metaFieldIndex.columns}")
            val intColumn = assertNotNull(metaFieldIndex.columns.find { it.name == "meta_int_value" })
            assertTrue(intColumn.asc)
        }
    }
}
