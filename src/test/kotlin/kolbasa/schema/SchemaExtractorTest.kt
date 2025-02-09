package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.queue.Unique
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.time.Duration
import kotlin.test.*

internal class SchemaExtractorTest : AbstractPostgresqlTest() {

    private val queueName = "test_queue"
    private val minValue = 0.toLong()
    private val maxValue = 9223372036854775807
    private val cacheValue = 1000.toLong()
    private val incrementValue = 1.toLong()

    data class SchemaExtractorTestMeta(
        val stringValue: String,
        val longValue: Long,
        @Unique
        val intValue: Int,
        val shortValue: Short,
        val booleanValue: Boolean,
        val doubleValue: Double,
        val floatValue: Float,
        val charValue: Char,
        val bigIntegerValue: BigInteger
    )

    private val testQueue = Queue(
        queueName,
        PredefinedDataTypes.String,
        options = QueueOptions(
            defaultDelay = Duration.ofMinutes(5),
            defaultAttempts = 42
        ),
        metadata = SchemaExtractorTestMeta::class.java
    )

    @BeforeTest
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, testQueue)
    }

    @Test
    fun testExtractRawSchema() {
        // here we have to find objects (tables, indexes etc.) only from 'public' schema
        val tables = SchemaExtractor.extractRawSchema(dataSource)

        assertEquals(1, tables.size, "Tables: ${tables.keys}")

        val testTable = assertNotNull(tables[testQueue.dbTableName])

        // check columns
        assertEquals(20, testTable.columns.size, "Found columns: ${testTable.columns}")

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
            assertFalse(scheduledAtColumn.nullable)
            assertNotNull(scheduledAtColumn.defaultExpression)
        }

        // attempts
        assertNotNull(testTable.findColumn("remaining_attempts")).let { attemptsColumn ->
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
            assertEquals(ColumnType.VARCHAR, metaCharValueColumn.type)
            assertTrue(metaCharValueColumn.nullable)
            assertNull(metaCharValueColumn.defaultExpression)
        }

        // meta_biginteger_value
        assertNotNull(testTable.findColumn("meta_big_integer_value")).let { metaBigIntegerValueColumn ->
            assertEquals(ColumnType.NUMERIC, metaBigIntegerValueColumn.type)
            assertTrue(metaBigIntegerValueColumn.nullable)
            assertNull(metaBigIntegerValueColumn.defaultExpression)
        }

        // Check indexes
        assertEquals(4, testTable.indexes.size, "Indexes: ${testTable.indexes}")

        // scheduled_at index
        assertNotNull(testTable.findIndex("${testQueue.dbTableName}_scheduled_at")).let { scheduledAtIndex ->
            assertFalse(scheduledAtIndex.unique)
            assertNull(scheduledAtIndex.filterCondition)
            assertFalse(scheduledAtIndex.invalid)
            assertEquals(1, scheduledAtIndex.columns.size, "Columns: ${scheduledAtIndex.columns}")
            val scheduledAtColumn = assertNotNull(scheduledAtIndex.columns.find { it.name == "scheduled_at" })
            assertTrue(scheduledAtColumn.asc)
        }

        // meta_int index
        assertNotNull(testTable.findIndex("${testQueue.dbTableName}_meta_int_value")).let { metaFieldIndex ->
            assertTrue(metaFieldIndex.unique)
            assertEquals("(remaining_attempts > 0)", metaFieldIndex.filterCondition)
            assertFalse(metaFieldIndex.invalid)
            assertEquals(1, metaFieldIndex.columns.size, "Columns: ${metaFieldIndex.columns}")
            val intColumn = assertNotNull(metaFieldIndex.columns.find { it.name == "meta_int_value" })
            assertTrue(intColumn.asc)
        }
    }
}
