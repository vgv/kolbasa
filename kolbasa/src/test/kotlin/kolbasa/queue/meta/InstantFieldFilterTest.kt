package kolbasa.queue.meta

import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.filter.Condition
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.neq
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import java.time.Instant

internal class InstantFieldFilterTest : AbstractPostgreSQLTest() {

    private val EVENT_TIME = MetaField.ofInstant("event_time", FieldOption.SEARCH)

    private val queue = Queue.of(
        "instant_filter",
        PredefinedDataTypes.String,
        metadata = Metadata.of(EVENT_TIME)
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    /**
     * A filter compares an [Instant] the way it was stored: cut to microseconds.
     *
     * The value carries nanoseconds, and the tail after the microseconds is 500 ns or more. kolbasa cuts it off when it
     * sends the message. If the filter value went to pgjdbc as it is, pgjdbc would round it up instead, and the filter
     * would look for the next microsecond, or even the next second for `.999999999`.
     *
     * Expected: `eq` finds the message by the value it was sent with, `neq` doesn't find it.
     */
    @ParameterizedTest
    @ValueSource(strings = ["2026-11-01T10:00:00.123456789Z", "2026-11-01T10:00:00.999999999Z"])
    fun testFilter_SameValueAsSent(value: String) {
        val eventTime = Instant.parse(value)
        DatabaseProducer(dataSource).send(queue, SendMessage("event", MetaValues.of(EVENT_TIME.value(eventTime))))

        assertEquals(1, receive(EVENT_TIME eq eventTime))
        assertEquals(0, receive(EVENT_TIME neq eventTime))
    }

    private fun receive(filter: Condition): Int {
        // A zero visibility timeout leaves the message visible for the next receive
        val options = ReceiveOptions(filter = filter, visibilityTimeout = Duration.ZERO)
        return DatabaseConsumer(dataSource).receive(queue, 10, options).size
    }
}
