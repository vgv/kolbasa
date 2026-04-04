package kolbasa.consumer.sweep

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.*
import kolbasa.schema.SchemaHelpers
import kolbasa.utils.JdbcHelpers.readInt
import kolbasa.utils.JdbcHelpers.useConnection
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

class RetentionCleanupTest : AbstractPostgresqlTest() {

    @Test
    fun testDlqRetentionCleanupByDuration() {
        val queue = Queue(
            name = "dlq_cleanup_by_duration",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(dlqOptions = DlqOptions(retention = Duration.ofHours(1)))
        )
        val dlq = requireNotNull(queue.deadLetterQueue)

        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        val producer = DatabaseProducer(dataSource)
        // Create messages with 1 attempt so they expire after receive
        val messages = (1..5).map {
            SendMessage("data_$it", messageOptions = MessageOptions(attempts = 1))
        }
        producer.send(queue, messages)

        // Receive to exhaust attempts
        val consumer = DatabaseConsumer(dataSource)
        consumer.receive(queue, limit = 5, receiveOptions = ReceiveOptions(visibilityTimeout = Duration.ZERO))

        // Trigger sweep manually — moves to DLQ
        dataSource.useConnection {
            SweepHelper.sweep(it, queue, 100)
        }
        assertEquals(5, dataSource.readInt("select count(*) from ${dlq.dbTableName}"))

        // Manually backdate DLQ messages to simulate expiration
        dataSource.useStatement { statement ->
            statement.executeUpdate("update ${dlq.dbTableName} set scheduled_at = scheduled_at - interval '2 hours'")
        }

        // Sweep again — should trigger retention cleanup on DLQ
        dataSource.useConnection {
            SweepHelper.sweep(it, queue, 100)
        }

        // All DLQ messages should be cleaned up (they're older than 1 hour retention)
        assertEquals(0, dataSource.readInt("select count(*) from ${dlq.dbTableName}"))
    }

    @Test
    fun testArchiveRetentionCleanupByDuration() {
        val queue = Queue(
            name = "archive_cleanup_by_duration",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(archiveQueueOptions = ArchiveQueueOptions(retention = Duration.ofHours(1)))
        )
        val archiveQueue = requireNotNull(queue.archiveQueue)

        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        val producer = DatabaseProducer(dataSource)
        (1..5).forEach { producer.send(queue, "data_$it") }

        // Receive and delete — moves to archive
        val consumer = DatabaseConsumer(dataSource)
        val received = consumer.receive(queue, limit = 5)
        consumer.delete(queue, received)
        assertEquals(5, dataSource.readInt("select count(*) from ${archiveQueue.dbTableName}"))

        // Backdate archive messages
        dataSource.useStatement { statement ->
            statement.executeUpdate("update ${archiveQueue.dbTableName} set scheduled_at = scheduled_at - interval '2 hours'")
        }

        // Sweep again to trigger retention on archive
        dataSource.useConnection { SweepHelper.sweep(it, queue, 100) }

        // Archive messages should be cleaned up
        assertEquals(0, dataSource.readInt("select count(*) from ${archiveQueue.dbTableName}"))
    }
}
