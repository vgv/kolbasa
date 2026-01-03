package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.Metadata
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.time.Duration
import kotlin.random.Random
import kotlin.system.measureTimeMillis

class SchemaHelpersTest : AbstractPostgresqlTest() {

    @Test
    fun testTime() {
        val queues = 1000
        val tablesSql = "select count(*) from information_schema.tables where table_schema='public'"
        val indexesSql = "select count(*) from pg_indexes where schemaname='public'"

        // 1000 queues with random metadata
        val randomQueues = (1..queues).map { index -> createRandomQueue(index) }

        // Sanity direct database check - empty database should have 0 tables and 0 indexes
        assertEquals(0, dataSource.readInt(tablesSql))
        assertEquals(0, dataSource.readInt(indexesSql))


        // ------------------------------------------------------------------------------------
        // First test - measure time to create all tables from empty database
        val emptyDatabaseMillis = measureTimeMillis {
            SchemaHelpers.createOrUpdateQueues(dataSource, randomQueues)
        }
        // Direct database checks, all tables and indexes should be created
        assertEquals(queues, dataSource.readInt(tablesSql))
        assertEquals(queues * 6, dataSource.readInt(indexesSql))
        //assertTrue(emptyDatabaseMillis < 10_000, "Creating $queues tables took too long: $emptyDatabaseMillis) ms")


        // ------------------------------------------------------------------------------------
        // Second test - drop half of the tables and measure time to create or update all tables again
        SchemaHelpers.deleteQueues(dataSource, randomQueues.shuffled().take(queues / 2))
        assertEquals(queues / 2, dataSource.readInt(tablesSql))
        assertEquals(queues * 3, dataSource.readInt(indexesSql))
        val halfDatabaseMillis = measureTimeMillis {
            SchemaHelpers.createOrUpdateQueues(dataSource, randomQueues)
        }
        // All tables and indexes should be created again
        assertEquals(queues, dataSource.readInt(tablesSql))
        assertEquals(queues * 6, dataSource.readInt(indexesSql))
        //assertTrue(halfDatabaseMillis < 5_000, "Creating $queues tables took too long: $halfDatabaseMillis) ms")


        // ------------------------------------------------------------------------------------
        // Third test - measure time to create or update all tables again without any changes
        val upToDateDatabaseMillis = measureTimeMillis {
            SchemaHelpers.createOrUpdateQueues(dataSource, randomQueues)
        }
        // All tables and indexes should remain the same
        assertEquals(queues, dataSource.readInt(tablesSql))
        assertEquals(queues * 6, dataSource.readInt(indexesSql))
        //assertTrue(upToDateDatabaseMillis < 1000, "Creating $queues tables took too long: $upToDateDatabaseMillis) ms")

        println("Creating $queues tables: empty database = ${emptyDatabaseMillis} ms, half database = ${halfDatabaseMillis} ms, up-to-date database = ${upToDateDatabaseMillis} ms")
    }

    @Test
    fun testRename() {
        val queue = Queue.of("test_queue", PredefinedDataTypes.String)

        // Create queue before renaming
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Direct database check that table exists
        val oldTableExistsQuery = """select count(*)
                     from information_schema.tables
                     where table_schema='public' and table_name='${queue.dbTableName}'
                  """
        assertEquals(1, dataSource.readInt(oldTableExistsQuery))

        // --------------------------------------------------------------------------------
        // Rename queue table
        val newQueueSuffix = "_renamed"
        var renameStatements = SchemaHelpers.renameQueues(dataSource, queue) { _ -> queue.name + newQueueSuffix }
        assertEquals(1, renameStatements)

        // More direct database checks
        val newTableExistsQuery = """select count(*)
                     from information_schema.tables
                     where table_schema='public' and table_name='${queue.dbTableName + newQueueSuffix}'
                  """
        assertEquals(0, dataSource.readInt(oldTableExistsQuery))
        assertEquals(1, dataSource.readInt(newTableExistsQuery))

        // --------------------------------------------------------------------------------
        // Try to rename again to the same name - should be no changes
        renameStatements = SchemaHelpers.renameQueues(dataSource, queue) { _ -> queue.name + newQueueSuffix }
        assertEquals(0, renameStatements) // because queue is already renamed

        // More direct database checks
        assertEquals(0, dataSource.readInt(oldTableExistsQuery))
        assertEquals(1, dataSource.readInt(newTableExistsQuery))
    }

    @Test
    fun testDelete() {
        val queue = Queue.of("test_queue", PredefinedDataTypes.String)

        // Create queue before renaming
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Direct database check that table exists
        val tableExistsQuery = """select count(*)
                     from information_schema.tables
                     where table_schema='public' and table_name='${queue.dbTableName}'
                  """
        assertEquals(1, dataSource.readInt(tableExistsQuery))

        // --------------------------------------------------------------------------------
        // Delete queue table
        var deleteStatements = SchemaHelpers.deleteQueues(dataSource, queue)
        assertEquals(1, deleteStatements)

        // One more direct database check
        assertEquals(0, dataSource.readInt(tableExistsQuery))

        // --------------------------------------------------------------------------------
        // Try to delete again - should be no changes
        deleteStatements = SchemaHelpers.deleteQueues(dataSource, queue)
        assertEquals(0, deleteStatements) // because queue table is already deleted

        // One more direct database check
        assertEquals(0, dataSource.readInt(tableExistsQuery))
    }

    @Test
    fun testGenerateSchema_CheckDurationBiggerThanMaxInt() {
        val duration = Duration.ofHours(24 * 365 * 1000) // approx. 1000 years

        val queue = Queue.builder("big_delay", PredefinedDataTypes.String)
            .options(QueueOptions(defaultDelay = duration, defaultVisibilityTimeout = duration))
            .build()

        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testExecuteSchemaStatements() {
        val statements = 1000
        // just 3 and 5 random numbers, it doesn't matter which ones
        val tableFailedIndexes = listOf(111, 222, 333)
        val indexFailedIndexes = listOf(234, 345, 456, 567, 678)

        val tableStatementsWithOneFailed = (1..statements).map { index ->
            if (index in tableFailedIndexes) {
                "INVALID SQL STATEMENT;"
            } else {
                "CREATE TABLE test_table_$index (id INT);"
            }
        }
        val indexStatementsWithFiveFailed = (1..statements).map { index ->
            if (index in indexFailedIndexes) {
                "INVALID SQL STATEMENT;"
            } else {
                "CREATE INDEX CONCURRENTLY index_test_table_$index ON test_table_1(id);"
            }
        }

        // Pre-check - database should be empty
        val tablesSql = "select count(*) from information_schema.tables where table_schema='public'"
        val indexesSql = "select count(*) from pg_indexes where schemaname='public'"
        assertEquals(0, dataSource.readInt(tablesSql))
        assertEquals(0, dataSource.readInt(indexesSql))

        // ------------------------------------------------------------------------------------
        // Execute statements with one invalid
        val schema = Schema(tableStatementsWithOneFailed, indexStatementsWithFiveFailed)
        val schemaResult = SchemaHelpers.executeSchemaStatements(dataSource, schema)

        assertSame(schema, schemaResult.schema)
        assertEquals(tableFailedIndexes.size + indexFailedIndexes.size, schemaResult.failedStatements)
        assertEquals(tableFailedIndexes.size, schemaResult.failedTableStatements.size)
        assertEquals(indexFailedIndexes.size, schemaResult.failedIndexStatements.size)
        // Direct database checks - all valid tables should be created
        assertEquals(statements - tableFailedIndexes.size, dataSource.readInt(tablesSql))
        assertEquals(statements - indexFailedIndexes.size, dataSource.readInt(indexesSql))
    }


    private fun createRandomQueue(number: Int): Queue<*> {
        val first = MetaField.int("first", FieldOption.SEARCH)
        val second = MetaField.long("second", FieldOption.STRICT_UNIQUE)
        val third = MetaField.string("third", FieldOption.PENDING_ONLY_UNIQUE)
        val fourth = MetaField.boolean("fourth")

        val databaseType = when (Random.nextInt(4)) {
            0 -> PredefinedDataTypes.String
            1 -> PredefinedDataTypes.ByteArray
            2 -> PredefinedDataTypes.Long
            3 -> PredefinedDataTypes.Int
            else -> fail("Unexpected number of data types")
        }

        return Queue.builder("test_queue_$number", databaseType)
            .metadata(Metadata.of(first, second, third, fourth))
            .build()
    }


}
