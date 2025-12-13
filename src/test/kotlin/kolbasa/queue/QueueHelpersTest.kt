package kolbasa.queue

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.MessageOptions
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendOptions
import kolbasa.schema.Const
import java.time.Duration
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

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
        // All default
        run {
            assertEquals(
                expected = QueueOptions.DEFAULT.defaultDelay,
                actual = QueueHelpers.calculateDelay(
                    QueueOptions(),
                    ProducerOptions.DEFAULT,
                    SendOptions.DEFAULT,
                    MessageOptions.DEFAULT
                )
            )
        }

        // Custom queue
        run {
            val duration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val options = QueueOptions(defaultDelay = duration)
            assertEquals(
                expected = duration,
                QueueHelpers.calculateDelay(options, ProducerOptions.DEFAULT, SendOptions.DEFAULT, MessageOptions.DEFAULT)
            )
        }

        // Custom producer
        run {
            val queueDuration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val producerDuration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val queueOptions = QueueOptions(defaultDelay = queueDuration)
            val producerOptions = ProducerOptions(delay = producerDuration)
            assertEquals(
                expected = producerDuration,
                QueueHelpers.calculateDelay(queueOptions, producerOptions, SendOptions.DEFAULT, MessageOptions.DEFAULT)
            )
        }

        // Custom send options
        run {
            val queueDuration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val producerDuration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val sendDuration = Duration.ofMillis(Random.nextLong(100, 10_000))

            val queueOptions = QueueOptions(defaultDelay = queueDuration)
            val producerOptions = ProducerOptions(delay = producerDuration)
            val sendOptions = SendOptions(delay = sendDuration)

            assertEquals(
                expected = sendDuration,
                QueueHelpers.calculateDelay(queueOptions, producerOptions, sendOptions, MessageOptions.DEFAULT)
            )
        }

        // Custom message options
        run {
            val queueDuration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val producerDuration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val sendDuration = Duration.ofMillis(Random.nextLong(100, 10_000))
            val messageDuration = Duration.ofMillis(Random.nextLong(100, 10_000))

            val queueOptions = QueueOptions(defaultDelay = queueDuration)
            val producerOptions = ProducerOptions(delay = producerDuration)
            val sendOptions = SendOptions(delay = sendDuration)
            val messageOptions = MessageOptions(delay = messageDuration)

            assertEquals(
                expected = messageDuration,
                QueueHelpers.calculateDelay(queueOptions, producerOptions, sendOptions, messageOptions)
            )
        }
    }

    @Test
    fun testCalculateAttempts() {
        // All default
        run {
            assertEquals(
                expected = QueueOptions.DEFAULT.defaultAttempts,
                actual = QueueHelpers.calculateAttempts(
                    QueueOptions(),
                    ProducerOptions.DEFAULT,
                    SendOptions.DEFAULT,
                    MessageOptions.DEFAULT
                )
            )
        }

        // Custom queue
        run {
            val queueAttempts = Random.nextInt(1, 100)
            val queueOptions = QueueOptions(defaultAttempts = queueAttempts)
            assertEquals(
                expected = queueAttempts,
                QueueHelpers.calculateAttempts(queueOptions, ProducerOptions.DEFAULT, SendOptions.DEFAULT, MessageOptions.DEFAULT)
            )
        }

        // Custom producer
        run {
            val queueAttempts = Random.nextInt(1, 100)
            val producerAttempts = Random.nextInt(1, 100)

            val queueOptions = QueueOptions(defaultAttempts = queueAttempts)
            val producerOptions = ProducerOptions(attempts = producerAttempts)

            assertEquals(
                expected = producerAttempts,
                QueueHelpers.calculateAttempts(queueOptions, producerOptions, SendOptions.DEFAULT, MessageOptions.DEFAULT)
            )
        }

        // Custom send options
        run {
            val queueAttempts = Random.nextInt(1, 100)
            val producerAttempts = Random.nextInt(1, 100)
            val sendAttempts = Random.nextInt(1, 100)

            val queueOptions = QueueOptions(defaultAttempts = queueAttempts)
            val producerOptions = ProducerOptions(attempts = producerAttempts)
            val sendOptions = SendOptions(attempts = sendAttempts)

            assertEquals(
                expected = sendAttempts,
                QueueHelpers.calculateAttempts(queueOptions, producerOptions, sendOptions, MessageOptions.DEFAULT)
            )
        }

        // Custom message options
        run {
            val queueAttempts = Random.nextInt(1, 100)
            val producerAttempts = Random.nextInt(1, 100)
            val sendAttempts = Random.nextInt(1, 100)
            val messageAttempts = Random.nextInt(1, 100)

            val queueOptions = QueueOptions(defaultAttempts = queueAttempts)
            val producerOptions = ProducerOptions(attempts = producerAttempts)
            val sendOptions = SendOptions(attempts = sendAttempts)
            val messageOptions = MessageOptions(attempts = messageAttempts)

            assertEquals(
                expected = messageAttempts,
                QueueHelpers.calculateAttempts(queueOptions, producerOptions, sendOptions, messageOptions)
            )
        }
    }

    @Test
    fun testCalculateVisibilityTimeout() {
        run {
            val options = QueueOptions()
            assertEquals(
                expected = QueueOptions.DEFAULT.defaultVisibilityTimeout,
                QueueHelpers.calculateVisibilityTimeout(options, ConsumerOptions.DEFAULT, ReceiveOptions.DEFAULT)
            )
        }

        run {
            val queueTimeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val queueOptions = QueueOptions(defaultVisibilityTimeout = queueTimeout)

            assertEquals(
                expected = queueTimeout,
                QueueHelpers.calculateVisibilityTimeout(queueOptions, ConsumerOptions.DEFAULT, ReceiveOptions.DEFAULT)
            )
        }

        run {
            val queueTimeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val queueOptions = QueueOptions(defaultVisibilityTimeout = queueTimeout)

            assertEquals(
                expected = queueTimeout,
                QueueHelpers.calculateVisibilityTimeout(queueOptions, ConsumerOptions(), ReceiveOptions.DEFAULT)
            )
        }

        run {
            val consumerTimeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val consumerOptions = ConsumerOptions(visibilityTimeout = consumerTimeout)
            assertEquals(
                expected = consumerTimeout,
                QueueHelpers.calculateVisibilityTimeout(QueueOptions(), consumerOptions, ReceiveOptions.DEFAULT)
            )
        }

        run {
            val queueTimeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val queueOptions = QueueOptions(defaultVisibilityTimeout = queueTimeout)

            assertEquals(
                expected = queueTimeout,
                QueueHelpers.calculateVisibilityTimeout(queueOptions, ConsumerOptions.DEFAULT, ReceiveOptions())
            )
        }

        run {
            val queueTimeout = Duration.ofMillis(Random.nextLong(100, 10_000))
            val consumerTimeout = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val receiveTimeout = Duration.ofMillis(Random.nextLong(30_000, 1_000_000_000))

            val queueOptions = QueueOptions(defaultVisibilityTimeout = queueTimeout)
            val consumerOptions = ConsumerOptions(visibilityTimeout = consumerTimeout)
            val receiveOptions = ReceiveOptions(visibilityTimeout = receiveTimeout)
            assertEquals(
                expected = receiveTimeout,
                QueueHelpers.calculateVisibilityTimeout(queueOptions, consumerOptions, receiveOptions)
            )
        }

        run {
            val receiveTimeout = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val options = QueueOptions()
            val receiveOptions = ReceiveOptions(visibilityTimeout = receiveTimeout)
            assertEquals(
                expected = receiveTimeout,
                QueueHelpers.calculateVisibilityTimeout(options, ConsumerOptions.DEFAULT, receiveOptions)
            )
        }
    }
}
