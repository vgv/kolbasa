package kolbasa.queue

import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.Metadata
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

class QueueTest {

    private val USER_ID = MetaField.int("user_id")
    private val NAME = MetaField.string("name")

    @Test
    fun testBuilders_WithoutMetadata_WithoutOptions() {
        val actual = Queue(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            options = null,
            metadata = Metadata.EMPTY
        )

        val expectedBuilder = Queue.builder("1", PredefinedDataTypes.String).build()
        val expectedOf = Queue.of("1", PredefinedDataTypes.String)

        assertEquals(expectedBuilder, actual)
        assertEquals(expectedOf, actual)
    }

    @Test
    fun testBuilders_WithMetadata() {
        val actual = Queue(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            options = null,
            metadata = Metadata.of(USER_ID, NAME)
        )

        val expectedBuilder = Queue.builder("1", PredefinedDataTypes.String)
            .metadata(Metadata.of(USER_ID, NAME))
            .build()
        val expectedOf = Queue.of("1", PredefinedDataTypes.String, Metadata.of(USER_ID, NAME))

        assertEquals(expectedBuilder, actual)
        assertEquals(expectedOf, actual)
    }

    @Test
    fun testBuilders_WithOptions() {
        val actual = Queue(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)),
            metadata = Metadata.EMPTY
        )

        val expected = Queue.builder("1", PredefinedDataTypes.String)
            .options(QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)))
            .build()

        assertEquals(expected, actual)
    }

    @Test
    fun testBuilders_WithMetadata_WithOptions() {
        val actual = Queue(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)),
            metadata = Metadata.of(USER_ID, NAME)
        )

        val expected = Queue.builder("1", PredefinedDataTypes.String)
            .metadata(Metadata.of(USER_ID, NAME))
            .options(QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)))
            .build()

        assertEquals(expected, actual)
    }

}
