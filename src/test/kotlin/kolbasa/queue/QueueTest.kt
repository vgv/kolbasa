package kolbasa.queue

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

class QueueTest {

    @Test
    fun testBuilders_WithoutMetadata_WithoutOptions() {
        val actual = Queue<String, Unit>(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            options = null,
            metadata = null
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
            metadata = TestMeta::class.java
        )

        val expectedBuilder = Queue.builder("1", PredefinedDataTypes.String)
            .metadata(TestMeta::class.java)
            .build()
        val expectedOf = Queue.of("1", PredefinedDataTypes.String, TestMeta::class.java)

        assertEquals(expectedBuilder, actual)
        assertEquals(expectedOf, actual)
    }

    @Test
    fun testBuilders_WithOptions() {
        val actual = Queue<String, Unit>(
            name = "1",
            databaseDataType = PredefinedDataTypes.String,
            options = QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)),
            metadata = null
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
            metadata = TestMeta::class.java
        )

        val expected = Queue.builder("1", PredefinedDataTypes.String)
            .metadata(TestMeta::class.java)
            .options(QueueOptions(Duration.ofSeconds(10), 42, Duration.ofHours(2)))
            .build()

        assertEquals(expected, actual)
    }

}

private class TestMeta(val userId: Int, val name: String)
