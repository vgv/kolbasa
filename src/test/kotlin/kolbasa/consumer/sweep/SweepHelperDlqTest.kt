package kolbasa.consumer.sweep

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.*
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers
import kolbasa.utils.JdbcHelpers.readInt
import kolbasa.utils.JdbcHelpers.useConnection
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class SweepHelperDlqTest : AbstractPostgresqlTest() {

    private val USER_ID = MetaField.int("user_id")

    private val queue = Queue(
        name = "dlq_test",
        databaseDataType = PredefinedDataTypes.String,
        metadata = Metadata.of(USER_ID),
        options = QueueOptions(dlqOptions = DlqOptions.DEFAULT)
    )

    private val dlq = requireNotNull(queue.deadLetterQueue)

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testSweepMovesDeadMessagesToDlq() {
        val producer = DatabaseProducer(dataSource)

        // 70 messages with 1 attempt, 30 with 10 attempts
        val messagesToSend = (1..100).map {
            val attempts = if (it <= 70) 1 else 10
            SendMessage("data_$it", messageOptions = MessageOptions(attempts = attempts))
        }
        producer.send(queue, messagesToSend)

        // Receive all with zero visibility timeout to exhaust attempts
        val consumer = DatabaseConsumer(dataSource)
        consumer.receive(
            queue,
            limit = 100,
            receiveOptions = ReceiveOptions(visibilityTimeout = Duration.ZERO)
        )

        // Trigger sweep manually to move messages with exhausted attempts to DLQ
        val removed = dataSource.useConnection {
            SweepHelper.sweep(it, queue, 100)
        }

        assertEquals(70, removed)
        // 30 remain in main queue
        assertEquals(30, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
        // 70 moved to DLQ
        assertEquals(70, dataSource.readInt("select count(*) from ${dlq.dbTableName}"))
    }

    @Test
    fun testSweepPreservesOriginalValues() {
        val producer = DatabaseProducer(dataSource)
        val meta = MetaValues.of(USER_ID.value(99))
        producer.send(queue, SendMessage("test_data", meta, MessageOptions(attempts = 1)))

        // Receive to exhaust attempts
        val consumer = DatabaseConsumer(dataSource)
        val message = requireNotNull(consumer.receive(queue, receiveOptions = ReceiveOptions(visibilityTimeout = Duration.ZERO)))

        // Trigger sweep manually to move messages with exhausted attempts to DLQ
        dataSource.useConnection { SweepHelper.sweep(it, queue, 100) }

        // Read from DLQ
        val dlqConsumer = DatabaseConsumer(dataSource)
        val dlqMsg = requireNotNull(dlqConsumer.receive(dlq, receiveOptions = ReceiveOptions(readMetadata = true)))

        assertEquals("test_data", dlqMsg.data)

        // Check original values is preserved in meta
        assertEquals(message.id.localId, dlqMsg.meta.get(Metadata.DLQ_ORIGINAL_ID))
        // TODO: fix timezone issues to enable these assertions
        //assertEquals(message.createdAt, dlqMsg.meta.get(Metadata.DLQ_ORIGINAL_CREATED_AT))
        //assertEquals(message.processingAt, dlqMsg.meta.get(Metadata.DLQ_ORIGINAL_PROCESSING_AT))
        //assertEquals(message.scheduledAt, dlqMsg.meta.get(Metadata.DLQ_ORIGINAL_SCHEDULED_AT))

        // Check user-defined meta is preserved
        assertEquals(99, dlqMsg.meta.get(USER_ID))
    }

    @Test
    fun testSweepWithoutDlq_PlainDelete() {
        val plainQueue = Queue.of("plain_sweep", PredefinedDataTypes.String)
        SchemaHelpers.createOrUpdateQueues(dataSource, plainQueue)

        val producer = DatabaseProducer(dataSource)
        val messagesToSend = (1..10).map {
            SendMessage("data_$it", messageOptions = MessageOptions(attempts = 1))
        }
        producer.send(plainQueue, messagesToSend)

        // Receive to exhaust attempts
        val consumer = DatabaseConsumer(dataSource)
        consumer.receive(
            plainQueue,
            limit = 10,
            receiveOptions = ReceiveOptions(visibilityTimeout = Duration.ZERO)
        )

        // Trigger sweep manually — plain delete, no DLQ
        val removed = dataSource.useConnection {
            SweepHelper.sweep(it, plainQueue, 100)
        }

        assertEquals(10, removed)
        assertEquals(0, dataSource.readInt("select count(*) from ${plainQueue.dbTableName}"))
    }
}
