package kolbasa.consumer.sweep

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration

class SweepHelperTest : AbstractPostgresqlTest() {

    private val queue = Queue.of("test", PredefinedDataTypes.String)

    @BeforeEach
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
    }

    @Test
    fun testCheckPeriod_IfAlwaysOn() {
        val queue = Queue.of("test", PredefinedDataTypes.String)
        val iterations = 1_000_000

        // Always false, if probability=0
        (1..iterations).forEach { _ ->
            assertTrue(SweepHelper.checkPeriod(queue, SweepConfig.EVERYTIME_SWEEP_PERIOD))
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [3, 5, 13, 25, 143, 567, 34523])
    fun testCheckPeriod(period: Int) {
        val queue = Queue.of("test_$period", PredefinedDataTypes.String)

        // Restart iterations, find next period
        while (!SweepHelper.checkPeriod(queue, period)) {
            // NOP
        }

        // Test
        (1..10).forEach { _ ->
            var iterations = 0
            while (!SweepHelper.checkPeriod(queue, period)) iterations++

            // Test that "false"
            assertEquals(period, iterations + 1)
        }
    }

    @Test
    fun testSweep() {
        // Prepare queue with 100 messages
        val producer = DatabaseProducer(dataSource)
        val messagesToSend = (1..100).map {
            // 70 messages with 1 attempt, 30 messages with 10 attempts
            val attempts = if (it <= 70) 1 else 10
            SendMessage("random_crap_$it", messageOptions = MessageOptions(attempts = attempts))
        }
        producer.send(queue, messagesToSend)

        // Receive all 100 mnessages with zero visibility timeout
        // It means that we will have 70 messages with 0 attempts (expired) and 30 messages with 9 attempts (not expired)
        val consumer = DatabaseConsumer(dataSource)
        val messages = consumer.receive(
            queue,
            limit = 100,
            receiveOptions = ReceiveOptions(visibilityTimeout = Duration.ZERO)
        )
        // Just flight check that we have received all messages, so, all messages have new attempts and visibility timeout values
        assertEquals(messagesToSend.size, messages.size)
        // ... and check that all messages are in the queue
        assertEquals(100, dataSource.readInt("select count(*) from ${queue.dbTableName}"))

        // Trigger sweep
        val removedMessages = dataSource.useConnection {
            SweepHelper.sweep(it, queue, 100)
        }
        assertEquals(70, removedMessages) // 70 messages must be removed
        assertEquals(30, dataSource.readInt("select count(*) from ${queue.dbTableName}")) // 30 messages must remain
    }

}
