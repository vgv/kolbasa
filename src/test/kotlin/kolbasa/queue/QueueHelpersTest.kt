package kolbasa.queue

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.MessageOptions
import kolbasa.schema.Const
import java.time.Duration
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

internal class QueueHelpersTest {

    @Test
    fun testGenerateDatabaseName() {
        assertEquals("abcd", QueueHelpers.generateDatabaseName("a", "b", "c", "d"))
        assertEquals("a-b-c-d", QueueHelpers.generateDatabaseName("a", "b", "c", "d", separator = "-"))
    }

    @Test
    fun testGenerateDatabaseName_TooLong() {
        val longName = "a".repeat(Const.MAX_DATABASE_OBJECT_NAME_LENGTH + 1)
        assertFailsWith<IllegalStateException> {
           QueueHelpers.generateDatabaseName(longName, longName, longName, separator = "-")
        }
    }

    @Test
    fun testGenerateQueueDbName() {
        val queueName = "my_queue"
        val expectedDbName = Const.QUEUE_TABLE_NAME_PREFIX + queueName
        assertEquals(expectedDbName, QueueHelpers.generateQueueDbName(queueName))
    }

    @Test
    fun testGenerateMetaColumnDbName() {
        val fieldName = "someFieldName"
        val expectedColumnName = Const.META_FIELD_NAME_PREFIX + "some_field_name"
        assertEquals(expectedColumnName, QueueHelpers.generateMetaColumnDbName(fieldName))
    }

    @Test
    fun testGenerateMetaColumnIndexName_Normal() {
        val queueName = "my_queue"
        val fieldName = "someField"
        val indexSuffix = "idx"
        val expectedIndexName = "my_queue_someField_idx"
        assertEquals(expectedIndexName, QueueHelpers.generateMetaColumnIndexName(queueName, fieldName, indexSuffix))
    }

    @Test
    fun testGenerateMetaColumnIndexName_Replace_Column_Name() {
        val queueName = "my_queue_quite_long_name_name_name_name_name"
        val fieldName = "someExtremelyLongFieldNameThatMayCauseTheIndexNameToBeTooLong"
        val indexSuffix = "idx"
        val expectedIndexName = "my_queue_quite_long_name_name_name_name_name_1ba1bfe469_idx"
        assertEquals(expectedIndexName, QueueHelpers.generateMetaColumnIndexName(queueName, fieldName, indexSuffix))
    }

    @Test
    fun testGenerateMetaColumnIndexName_Replace_Everything() {
        val queueName = "my_queue_quite_long_name_name_name_name_name_name__name_name"
        val fieldName = "someExtremelyLongFieldNameThatMayCauseTheIndexNameToBeTooLong"
        val indexSuffix = "idx"
        val expectedIndexName = "idx_b55a442ae34d130843c1bc90904acdbd_idx"
        assertEquals(expectedIndexName, QueueHelpers.generateMetaColumnIndexName(queueName, fieldName, indexSuffix))
    }

    @Test
    fun testCalculateDelay() {
        assertNull(QueueHelpers.calculateDelay(null, null))

        run {
            val options = QueueOptions()
            assertEquals(QueueOptions.DEFAULT_DELAY, QueueHelpers.calculateDelay(options, null))
        }

        run {
            val duration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val options = QueueOptions(defaultDelay = duration)
            assertEquals(duration, QueueHelpers.calculateDelay(options, null))
        }

        run {
            val duration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val options = QueueOptions(defaultDelay = duration)
            val messageOptions = MessageOptions()
            assertEquals(duration, QueueHelpers.calculateDelay(options, messageOptions))
        }

        run {
            val duration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val sendDuration = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val options = QueueOptions(defaultDelay = duration)
            val messageOptions = MessageOptions(delay = sendDuration)
            assertEquals(sendDuration, QueueHelpers.calculateDelay(options, messageOptions))
        }

        run {
            val sendDuration = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val messageOptions = MessageOptions(delay = sendDuration)
            assertEquals(sendDuration, QueueHelpers.calculateDelay(null, messageOptions))
        }

        run {
            val sendDuration = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val options = QueueOptions()
            val messageOptions = MessageOptions(delay = sendDuration)
            assertEquals(sendDuration, QueueHelpers.calculateDelay(options, messageOptions))
        }
    }

    @Test
    fun testCalculateAttempts() {
        assertEquals(QueueOptions.DEFAULT_ATTEMPTS, QueueHelpers.calculateAttempts(null, null))

        run {
            val options = QueueOptions()
            assertEquals(QueueOptions.DEFAULT_ATTEMPTS, QueueHelpers.calculateAttempts(options, null))
        }

        run {
            val attempts = Random.nextInt(10, 100)
            val options = QueueOptions(defaultAttempts = attempts)
            assertEquals(attempts, QueueHelpers.calculateAttempts(options, null))
        }

        run {
            val attempts = Random.nextInt(10, 100)
            val options = QueueOptions(defaultAttempts = attempts)
            val messageOptions = MessageOptions()
            assertEquals(attempts, QueueHelpers.calculateAttempts(options, messageOptions))
        }

        run {
            val attempts = Random.nextInt(10, 100)
            val sendAttempts = Random.nextInt(200, 300)
            val options = QueueOptions(defaultAttempts = attempts)
            val messageOptions = MessageOptions(attempts = sendAttempts)
            assertEquals(sendAttempts, QueueHelpers.calculateAttempts(options, messageOptions))
        }

        run {
            val sendAttempts = Random.nextInt(200, 300)
            val options = QueueOptions()
            val messageOptions = MessageOptions(attempts = sendAttempts)
            assertEquals(sendAttempts, QueueHelpers.calculateAttempts(options, messageOptions))
        }

        run {
            val sendAttempts = Random.nextInt(200, 300)
            val messageOptions = MessageOptions(attempts = sendAttempts)
            assertEquals(sendAttempts, QueueHelpers.calculateAttempts(null, messageOptions))
        }
    }

    @Test
    fun testCalculateVisibilityTimeout() {
        assertEquals(
            QueueOptions.DEFAULT_VISIBILITY_TIMEOUT, QueueHelpers.calculateVisibilityTimeout(
                null,
                null,
                null
            )
        )

        run {
            val options = QueueOptions()
            assertEquals(
                QueueOptions.DEFAULT_VISIBILITY_TIMEOUT, QueueHelpers.calculateVisibilityTimeout(
                    options,
                    null,
                    null
                )
            )
        }

        run {
            val timeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val options = QueueOptions(defaultVisibilityTimeout = timeout)
            assertEquals(timeout, QueueHelpers.calculateVisibilityTimeout(options, null, null))
        }

        run {
            val timeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val options = QueueOptions(defaultVisibilityTimeout = timeout)
            val consumerOptions = ConsumerOptions()
            assertEquals(timeout, QueueHelpers.calculateVisibilityTimeout(options, consumerOptions, null))
        }

        run {
            val timeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val options = QueueOptions()
            val consumerOptions = ConsumerOptions(visibilityTimeout = timeout)
            assertEquals(timeout, QueueHelpers.calculateVisibilityTimeout(options, consumerOptions, null))
        }

        run {
            val timeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val options = QueueOptions(defaultVisibilityTimeout = timeout)
            val receiveOptions = ReceiveOptions()
            assertEquals(timeout, QueueHelpers.calculateVisibilityTimeout(options, null, receiveOptions))
        }

        run {
            val timeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val consumerTimeout = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val receiveTimeout = Duration.ofMillis(Random.nextLong(30_000, 1_000_000_000))
            val options = QueueOptions(defaultVisibilityTimeout = timeout)
            val consumerOptions = ConsumerOptions(visibilityTimeout = consumerTimeout)
            val receiveOptions = ReceiveOptions(visibilityTimeout = receiveTimeout)
            assertEquals(
                receiveTimeout, QueueHelpers.calculateVisibilityTimeout(
                    options,
                    consumerOptions,
                    receiveOptions
                )
            )
        }

        run {
            val receiveTimeout = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val options = QueueOptions()
            val receiveOptions = ReceiveOptions(visibilityTimeout = receiveTimeout)
            assertEquals(
                receiveTimeout, QueueHelpers.calculateVisibilityTimeout(
                    options,
                    null,
                    receiveOptions
                )
            )
        }

        run {
            val receiveTimeout = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val receiveOptions = ReceiveOptions(visibilityTimeout = receiveTimeout)
            assertEquals(receiveTimeout, QueueHelpers.calculateVisibilityTimeout(null, null, receiveOptions))
        }
    }
}
