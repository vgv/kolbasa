package kolbasa.queue

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.MessageOptions
import kolbasa.schema.Const
import java.time.Duration

internal object QueueHelpers {

    fun generateDatabaseName(vararg parts: String, separator: String = ""): String {
        val result = parts.joinToString(separator = separator)

        check(result.length <= Const.MAX_DATABASE_OBJECT_NAME_LENGTH) {
            "Your identifier name is too long ($result). Max allowed length is ${Const.MAX_DATABASE_OBJECT_NAME_LENGTH} symbols"
        }

        return result
    }

    fun generateQueueDbName(name: String): String {
        return generateDatabaseName(Const.QUEUE_TABLE_NAME_PREFIX, name)
    }

    fun generateMetaColumnDbName(name: String): String {
        return generateDatabaseName(Const.META_FIELD_NAME_PREFIX, name)
    }

    fun calculateDelay(queueOptions: QueueOptions?, messageOptions: MessageOptions?): Duration? {
        var delay = queueOptions?.defaultDelay
        if (messageOptions != null) {
            if (messageOptions.delay !== QueueOptions.DELAY_NOT_SET) {
                delay = messageOptions.delay
            }
        }

        return delay
    }

    fun calculateAttempts(queueOptions: QueueOptions?, messageOptions: MessageOptions?): Int {
        var attempts = queueOptions?.defaultAttempts ?: QueueOptions.DEFAULT_ATTEMPTS
        if (messageOptions != null) {
            if (messageOptions.attempts != QueueOptions.ATTEMPTS_NOT_SET) {
                attempts = messageOptions.attempts
            }
        }

        return attempts
    }

    fun calculateVisibilityTimeout(
        queueOptions: QueueOptions?,
        consumerOptions: ConsumerOptions?,
        receiveOptions: ReceiveOptions?
    ): Duration {
        var timeout = queueOptions?.defaultVisibilityTimeout ?: QueueOptions.DEFAULT_VISIBILITY_TIMEOUT

        if (consumerOptions != null) {
            if (consumerOptions.visibilityTimeout !== QueueOptions.VISIBILITY_TIMEOUT_NOT_SET) {
                timeout = consumerOptions.visibilityTimeout
            }
        }

        if (receiveOptions != null) {
            if (receiveOptions.visibilityTimeout !== QueueOptions.VISIBILITY_TIMEOUT_NOT_SET) {
                timeout = receiveOptions.visibilityTimeout
            }
        }

        return timeout
    }

}
