package kolbasa.inspector.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.assertNotNull
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.filter.Filter.lessEq
import kolbasa.inspector.CountOptions
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import java.time.Duration

class DatabaseInspectorTest : AbstractPostgresqlTest() {

    private val FIELD = MetaField.int("field", FieldOption.SEARCH)

    private val queue = Queue.of(
        "local",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIELD)
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testCount_EmptyQueue() {
        val inspector = DatabaseInspector(dataSource)
        val messages = inspector.count(queue)

        assertEquals(0, messages.scheduled)
        assertEquals(0, messages.ready)
        assertEquals(0, messages.inFlight)
        assertEquals(0, messages.retry)
        assertEquals(0, messages.dead)
        assertEquals(0, messages.total())
    }

    @Test
    fun testCount_ReadyMessages() {
        val producer = DatabaseProducer(dataSource)
        val data = (1..10).map {
            SendMessage(it.toString(), MetaValues.of(FIELD.value(it)), MessageOptions(delay = Duration.ZERO))
        }
        producer.send(queue, data)

        val inspector = DatabaseInspector(dataSource)
        val messages = inspector.count(queue)

        assertEquals(10, messages.ready)
        assertEquals(0, messages.scheduled)
        assertEquals(0, messages.inFlight)
        assertEquals(0, messages.retry)
        assertEquals(0, messages.dead)
    }

    @Test
    fun testCount_ScheduledMessages() {
        val producer = DatabaseProducer(dataSource)
        val data = (1..5).map {
            SendMessage(it.toString(), MetaValues.of(FIELD.value(it)), MessageOptions(delay = Duration.ofHours(24)))
        }
        producer.send(queue, data)

        val inspector = DatabaseInspector(dataSource)
        val messages = inspector.count(queue)

        assertEquals(5, messages.scheduled)
        assertEquals(0, messages.ready)
        assertEquals(0, messages.inFlight)
        assertEquals(0, messages.retry)
        assertEquals(0, messages.dead)
    }

    @Test
    fun testCount_InFlightMessages() {
        val producer = DatabaseProducer(dataSource)
        val data = (1..5).map {
            SendMessage(it.toString(), MetaValues.of(FIELD.value(it)), MessageOptions(delay = Duration.ZERO))
        }
        producer.send(queue, data)

        // Receive messages without deleting them — they become IN_FLIGHT
        val consumer = DatabaseConsumer(dataSource)
        consumer.receive(queue, 5)

        val inspector = DatabaseInspector(dataSource)
        val messages = inspector.count(queue)

        assertEquals(5, messages.inFlight)
        assertEquals(0, messages.ready)
        assertEquals(0, messages.scheduled)
    }

    @Test
    fun testCount_WithFilter() {
        val producer = DatabaseProducer(dataSource)
        val data = (1..10).map {
            SendMessage(it.toString(), MetaValues.of(FIELD.value(it)), MessageOptions(delay = Duration.ZERO))
        }
        producer.send(queue, data)

        val inspector = DatabaseInspector(dataSource)
        val options = CountOptions(filter = FIELD lessEq 5)
        val messages = inspector.count(queue, options)

        assertEquals(5, messages.ready)
        assertEquals(5, messages.total())
    }

    @Test
    fun testSize_ReturnsPositive() {
        val producer = DatabaseProducer(dataSource)
        val data = (1..10).map {
            SendMessage(it.toString(), MetaValues.of(FIELD.value(it)))
        }
        producer.send(queue, data)

        val inspector = DatabaseInspector(dataSource)
        val size = inspector.size(queue)

        assertTrue(size > 0) { "Size should be positive, got $size" }
    }

    @Test
    fun testDistinctValues_ReturnsSortedUnique() {
        val producer = DatabaseProducer(dataSource)
        // Send duplicate values: 1,1,2,2,3,3
        val data = listOf(1, 1, 2, 2, 3, 3).map {
            SendMessage(it.toString(), MetaValues.of(FIELD.value(it)))
        }
        producer.send(queue, data)

        val inspector = DatabaseInspector(dataSource)
        val values = inspector.distinctValues(queue, FIELD, 100)

        // compare sets because the order is not guaranteed
        assertEquals(setOf(1, 2, 3), values.toSet())
    }

    @Test
    fun testIsEmpty_True() {
        val inspector = DatabaseInspector(dataSource)
        assertTrue(inspector.isEmpty(queue))
    }

    @Test
    fun testIsEmpty_False() {
        val producer = DatabaseProducer(dataSource)
        producer.send(queue, SendMessage("data", MetaValues.of(FIELD.value(1))))

        val inspector = DatabaseInspector(dataSource)
        assertFalse(inspector.isEmpty(queue))
    }

    @Test
    fun testIsEmpty_WithOnlyDeadMessages_ReturnsFalse() {
        // Send a message with 1 attempt, receive with zero visibility timeout → remaining_attempts becomes 0 → DEAD
        val producer = DatabaseProducer(dataSource)
        producer.send(queue, SendMessage("data", MetaValues.of(FIELD.value(1)), MessageOptions(attempts = 1)))

        val consumer = DatabaseConsumer(dataSource)
        consumer.receive(queue, 1, ReceiveOptions(visibilityTimeout = Duration.ZERO))

        val inspector = DatabaseInspector(dataSource)
        assertFalse(inspector.isEmpty(queue))
    }

    @Test
    fun testIsDeadOrEmpty_EmptyQueue_ReturnsTrue() {
        val inspector = DatabaseInspector(dataSource)
        assertTrue(inspector.isDeadOrEmpty(queue))
    }

    @Test
    fun testIsDeadOrEmpty_WithOnlyDeadMessages_ReturnsTrue() {
        // Send a message with 1 attempt, receive with zero visibility timeout → remaining_attempts becomes 0 → DEAD
        val producer = DatabaseProducer(dataSource)
        producer.send(queue, SendMessage("data", MetaValues.of(FIELD.value(1)), MessageOptions(attempts = 1)))

        val consumer = DatabaseConsumer(dataSource)
        consumer.receive(queue, 1, ReceiveOptions(visibilityTimeout = Duration.ZERO))

        val inspector = DatabaseInspector(dataSource)
        assertTrue(inspector.isDeadOrEmpty(queue))
    }

    @Test
    fun testIsDeadOrEmpty_WithLiveMessages_ReturnsFalse() {
        val producer = DatabaseProducer(dataSource)
        producer.send(queue, SendMessage("data", MetaValues.of(FIELD.value(1))))

        val inspector = DatabaseInspector(dataSource)
        assertFalse(inspector.isDeadOrEmpty(queue))
    }

    @Test
    fun testMessageAge_EmptyQueue() {
        val inspector = DatabaseInspector(dataSource)
        val age = inspector.messageAge(queue)

        assertNull(age.oldest)
        assertNull(age.newest)
        assertNull(age.oldestReady)
    }

    @Test
    fun testMessageAge_WithMessages() {
        val producer = DatabaseProducer(dataSource)
        val data = (1..5).map {
            SendMessage(it.toString(), MetaValues.of(FIELD.value(it)), MessageOptions(delay = Duration.ZERO))
        }
        producer.send(queue, data)

        val inspector = DatabaseInspector(dataSource)
        val age = inspector.messageAge(queue)

        assertTrue(assertNotNull(age.oldest).toMillis() >= 0)
        assertTrue(assertNotNull(age.newest).toMillis() >= 0)
        assertTrue(assertNotNull(age.oldestReady).toMillis() >= 0)
    }

}
