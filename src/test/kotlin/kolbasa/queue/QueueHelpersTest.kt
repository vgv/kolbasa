package kolbasa.queue

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.MessageOptions
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.time.Duration
import kotlin.random.Random

internal class QueueHelpersTest {

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
        assertEquals(QueueOptions.DEFAULT_VISIBILITY_TIMEOUT, QueueHelpers.calculateVisibilityTimeout(
            null,
            null,
            null
        ))

        run {
            val options = QueueOptions()
            assertEquals(QueueOptions.DEFAULT_VISIBILITY_TIMEOUT, QueueHelpers.calculateVisibilityTimeout(
                options,
                null,
                null
            ))
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
            assertEquals(receiveTimeout, QueueHelpers.calculateVisibilityTimeout(
                options,
                consumerOptions,
                receiveOptions
            ))
        }

        run {
            val receiveTimeout = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val options = QueueOptions()
            val receiveOptions = ReceiveOptions(visibilityTimeout = receiveTimeout)
            assertEquals(receiveTimeout, QueueHelpers.calculateVisibilityTimeout(
                options,
                null,
                receiveOptions
            ))
        }

        run {
            val receiveTimeout = Duration.ofMillis(Random.nextLong(20_000, 1_000_000))
            val receiveOptions = ReceiveOptions(visibilityTimeout = receiveTimeout)
            assertEquals(receiveTimeout, QueueHelpers.calculateVisibilityTimeout(null, null, receiveOptions))
        }
    }
}
