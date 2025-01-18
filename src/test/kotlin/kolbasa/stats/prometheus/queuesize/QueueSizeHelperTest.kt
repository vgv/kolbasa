package kolbasa.stats.prometheus.queuesize

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.pg.DatabaseExtensions.useConnectionWithAutocommit
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals

class QueueSizeHelperTest : AbstractPostgresqlTest() {

    @Test
    fun testCalculateQueueLength_TableNotExists() {
        val queue = Queue<String, Unit>("not_real_queue", PredefinedDataTypes.String)

        // Measure table size
        val length = dataSource.useConnection { QueueSizeHelper.calculateQueueLength(it, queue) }
        assertEquals(Const.TABLE_DOES_NOT_EXIST, length)
    }

    @Test
    fun testCalculateQueueLength_EmptyTableWithoutVacuum() {
        val queue = Queue<String, Unit>("real_queue", PredefinedDataTypes.String)
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)

        // Measure table size
        val length = dataSource.useConnection { QueueSizeHelper.calculateQueueLength(it, queue) }
        if (RANDOM_POSTGRES_IMAGE.modernVacuumStats) {
            assertEquals(Const.TABLE_HAS_NEVER_BEEN_VACUUMED, length)
        } else {
            assertEquals(0L, length)
        }
    }

    @Test
    fun testCalculateQueueLength_FullTableWithoutVacuum() {
        val queue = Queue<String, Unit>("real_queue", PredefinedDataTypes.String)
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)

        val producer = DatabaseProducer(dataSource)
        producer.send(queue, "1")
        producer.send(queue, "2")
        producer.send(queue, "3")

        // Measure table size
        // Even if we have inserted 3 records, the table size is still -1 (or 0 on old PG) because we haven't run vacuum yet
        val length = dataSource.useConnection { QueueSizeHelper.calculateQueueLength(it, queue) }
        if (RANDOM_POSTGRES_IMAGE.modernVacuumStats) {
            assertEquals(Const.TABLE_HAS_NEVER_BEEN_VACUUMED, length)
        } else {
            assertEquals(0L, length)
        }
    }

    @Test
    fun testCalculateQueueLength_EmptyTableWithVacuum() {
        val queue = Queue<String, Unit>("real_queue", PredefinedDataTypes.String)
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)

        // Run vacuum to update stats
        fullVacuum(dataSource, queue.dbTableName)

        // Measure table size
        val length = dataSource.useConnection { QueueSizeHelper.calculateQueueLength(it, queue) }
        assertEquals(0L, length)
    }

    @Test
    fun testCalculateQueueLength_FullTableWithVacuum() {
        val queue = Queue<String, Unit>("real_queue", PredefinedDataTypes.String)
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)

        val producer = DatabaseProducer(dataSource)
        producer.send(queue, "1")
        producer.send(queue, "2")
        producer.send(queue, "3")

        // Run vacuum after insert to update stats
        fullVacuum(dataSource, queue.dbTableName)

        // Measure table size
        val length = dataSource.useConnection { QueueSizeHelper.calculateQueueLength(it, queue) }
        assertEquals(3L, length)
    }

    private fun fullVacuum(dataSource: DataSource, tableName: String) {
        // vacuum full runs only in auto-commit mode
        dataSource.useConnectionWithAutocommit { connection ->
            connection.useStatement { it.execute("vacuum full $tableName") }
        }
    }

}
