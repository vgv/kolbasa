package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.queue.*
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.Metadata
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.time.Duration
import kotlin.random.Random
import kotlin.system.measureTimeMillis

class SchemaHelpersTest : AbstractPostgresqlTest() {

    @Test
    @Disabled
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
        assertTrue(emptyDatabaseMillis < 12_000, "Creating $queues tables took too long: $emptyDatabaseMillis) ms")


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
        assertTrue(halfDatabaseMillis < 6_000, "Creating $queues tables took too long: $halfDatabaseMillis) ms")


        // ------------------------------------------------------------------------------------
        // Third test - measure time to create or update all tables again without any changes
        val upToDateDatabaseMillis = measureTimeMillis {
            SchemaHelpers.createOrUpdateQueues(dataSource, randomQueues)
        }
        // All tables and indexes should remain the same
        assertEquals(queues, dataSource.readInt(tablesSql))
        assertEquals(queues * 6, dataSource.readInt(indexesSql))
        assertTrue(upToDateDatabaseMillis < 1000, "Creating $queues tables took too long: $upToDateDatabaseMillis) ms")
    }

    @Test
    fun testRename_WithoutCompanions() {
        val queue = Queue.of("test_queue", PredefinedDataTypes.String)

        // Create queue before renaming
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Direct database check that table exists
        assertTrue(tableExists(queue.dbTableName))

        // --------------------------------------------------------------------------------
        // Rename queue table
        val newQueueSuffix = "_renamed"
        var renameResult = SchemaHelpers.renameQueues(dataSource, queue) { _ -> queue.name + newQueueSuffix }
        assertEquals(0, renameResult.failedStatements)
        assertEquals(1, renameResult.schema.tableStatements.size)

        // Old table should be gone, new table should exist
        assertFalse(tableExists(queue.dbTableName))
        assertTrue(tableExists(queue.dbTableName + newQueueSuffix))

        // --------------------------------------------------------------------------------
        // Try to rename again to the same name - should be no changes
        renameResult = SchemaHelpers.renameQueues(dataSource, queue) { _ -> queue.name + newQueueSuffix }
        assertEquals(0, renameResult.failedStatements)
        assertTrue(renameResult.schema.isEmpty) // because queue is already renamed

        // Nothing should change - old table doesn't exist, new table still exists
        assertFalse(tableExists(queue.dbTableName))
        assertTrue(tableExists(queue.dbTableName + newQueueSuffix))
    }

    @Test
    fun testRename_With_DLQ_And_Archive() {
        val queue = Queue(
            name = "test_queue",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(
                dlqOptions = DlqOptions.DEFAULT,
                archiveQueueOptions = ArchiveQueueOptions.DEFAULT
            )
        )
        val dlq = requireNotNull(queue.deadLetterQueue)
        val archive = requireNotNull(queue.archiveQueue)

        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Before renaming - all tables should exist
        assertTrue(tableExists(queue.dbTableName))
        assertTrue(tableExists(dlq.dbTableName))
        assertTrue(tableExists(archive.dbTableName))

        // Ok, try to rename
        val newName = "renamed_queue"
        val renameResult = SchemaHelpers.renameQueues(dataSource, queue) { newName }
        assertEquals(0, renameResult.failedStatements)

        // Old tables gone
        assertFalse(tableExists(queue.dbTableName))
        assertFalse(tableExists(dlq.dbTableName))
        assertFalse(tableExists(archive.dbTableName))

        // New tables exist
        val newMainTable = QueueHelpers.generateQueueDbName(newName)
        val newDlqTable = QueueHelpers.generateQueueDbName(newName + Const.DLQ_TABLE_NAME_SUFFIX)
        val newArcTable = QueueHelpers.generateQueueDbName(newName + Const.ARCHIVE_TABLE_NAME_SUFFIX)
        assertTrue(tableExists(newMainTable))
        assertTrue(tableExists(newDlqTable))
        assertTrue(tableExists(newArcTable))

        // Rename again — should be no-op
        val renameAgain = SchemaHelpers.renameQueues(dataSource, queue) { newName }
        assertEquals(0, renameAgain.failedStatements)
        assertTrue(renameAgain.schema.isEmpty)
    }


    @Test
    fun testDelete() {
        val queue = Queue.of("test_queue", PredefinedDataTypes.String)

        // Create queue before renaming
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Direct database check that table exists
        assertTrue(tableExists(queue.dbTableName))

        // --------------------------------------------------------------------------------
        // Delete queue table
        var deleteResult = SchemaHelpers.deleteQueues(dataSource, queue)
        assertEquals(0, deleteResult.failedStatements)
        assertEquals(1, deleteResult.schema.tableStatements.size)

        // One more direct database check
        assertFalse(tableExists(queue.dbTableName))

        // --------------------------------------------------------------------------------
        // Try to delete again - should be no changes
        deleteResult = SchemaHelpers.deleteQueues(dataSource, queue)
        assertEquals(0, deleteResult.failedStatements)
        assertTrue(deleteResult.schema.isEmpty) // because queue table is already deleted

        // One more direct database check
        assertFalse(tableExists(queue.dbTableName))
    }

    @Test
    fun testDelete_With_DLQ_And_Archive() {
        val queue = Queue(
            name = "test_queue",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(
                dlqOptions = DlqOptions.DEFAULT,
                archiveQueueOptions = ArchiveQueueOptions.DEFAULT
            )
        )
        val dlq = requireNotNull(queue.deadLetterQueue)
        val archive = requireNotNull(queue.archiveQueue)

        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Before deleting - all tables should exist
        assertTrue(tableExists(queue.dbTableName))
        assertTrue(tableExists(dlq.dbTableName))
        assertTrue(tableExists(archive.dbTableName))

        // Ok, try to delete
        val deleteResult = SchemaHelpers.deleteQueues(dataSource, queue)
        assertEquals(0, deleteResult.failedStatements)

        // All tables should be gone
        assertFalse(tableExists(queue.dbTableName))
        assertFalse(tableExists(dlq.dbTableName))
        assertFalse(tableExists(archive.dbTableName))

        // Delete again — should be no-op
        val deleteAgain = SchemaHelpers.deleteQueues(dataSource, queue)
        assertEquals(0, deleteAgain.failedStatements)
        assertTrue(deleteAgain.schema.isEmpty)
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

    // ---- Helpers ----

    private fun tableExists(tableName: String): Boolean {
        val query = """select count(*)
            from information_schema.tables
            where table_schema='public' and table_name='$tableName'
        """
        return dataSource.readInt(query) == 1
    }

    private fun createRandomQueue(number: Int): Queue<*> {
        val first = MetaField.int("first", FieldOption.SEARCH)
        val second = MetaField.long("second", FieldOption.ALL_LIVE_UNIQUE)
        val third = MetaField.string("third", FieldOption.UNTOUCHED_UNIQUE)
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
