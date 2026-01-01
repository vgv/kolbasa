package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

class SchemaHelpersTest : AbstractPostgresqlTest() {

    private val queue = Queue.of("test_queue", PredefinedDataTypes.String)

    @Test
    fun testRename() {
        // Create queue before renaming
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Direct database check that table exists
        val oldTableExistsQuery = """select count(*)
                     from information_schema.tables
                     where table_schema='public' and table_name='${queue.dbTableName}'
                  """
        assertEquals(1, dataSource.readInt(oldTableExistsQuery))

        // Rename queue table
        val newQueueSuffix = "_renamed"
        SchemaHelpers.renameQueues(dataSource, queue) { _ -> queue.name + newQueueSuffix }

        // More direct database checks
        val newTableExistsQuery = """select count(*)
                     from information_schema.tables
                     where table_schema='public' and table_name='${queue.dbTableName + newQueueSuffix}'
                  """
        assertEquals(0, dataSource.readInt(oldTableExistsQuery))
        assertEquals(1, dataSource.readInt(newTableExistsQuery))
    }

    @Test
    fun testDelete() {
        // Create queue before renaming
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Direct database check that table exists
        val tableExistsQuery = """select count(*)
                     from information_schema.tables
                     where table_schema='public' and table_name='${queue.dbTableName}'
                  """
        assertEquals(1, dataSource.readInt(tableExistsQuery))

        // Delete queue table
        SchemaHelpers.deleteQueues(dataSource, queue)

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


}
