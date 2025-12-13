package kolbasa.queue

import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.ReceiveOptions
import kolbasa.producer.MessageOptions
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendOptions
import kolbasa.schema.Const
import kolbasa.utils.Helpers
import java.time.Duration

internal object QueueHelpers {

    private val META_COLUMN_REGEX = Regex("([a-z])([A-Z]+)")

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

    fun generateMetaColumnDbName(fieldName: String): String {
        // convert Java field into column name, like someField -> some_field
        val snakeCaseName = fieldName.replace(META_COLUMN_REGEX, "$1_$2").lowercase()

        // add 'meta_' prefix
        return generateDatabaseName(Const.META_FIELD_NAME_PREFIX, snakeCaseName)
    }

    fun generateMetaColumnIndexName(queueName: String, fieldName: String, indexSuffix: String): String {
        return try {
            // Queue name + field name + index suffix is less than max length, everything is ok
            generateDatabaseName(queueName, fieldName, indexSuffix, separator = "_")
        } catch (_: IllegalStateException) {
            try {
                // Try to keep the queue name and use short hash for field name
                generateDatabaseName(queueName, Helpers.shortHash(fieldName), indexSuffix, separator = "_")
            } catch (_: IllegalStateException) {
                // Even a queue name is too long, use hash for both queue name and field name
                generateDatabaseName("idx", Helpers.md5Hash(queueName + fieldName), indexSuffix, separator = "_")
            }
        }
    }

    fun calculateDelay(
        queueOptions: QueueOptions,
        producerOptions: ProducerOptions,
        sendOptions: SendOptions,
        messageOptions: MessageOptions
    ): Duration {
        if (messageOptions.delay != null) {
            return messageOptions.delay
        }

        if (sendOptions.delay != null) {
            return sendOptions.delay
        }

        if (producerOptions.delay != null) {
            return producerOptions.delay
        }

        return queueOptions.defaultDelay
    }

    fun calculateAttempts(
        queueOptions: QueueOptions,
        producerOptions: ProducerOptions,
        sendOptions: SendOptions,
        messageOptions: MessageOptions
    ): Int {
        if (messageOptions.attempts != null) {
            return messageOptions.attempts
        }

        if (sendOptions.attempts != null) {
            return sendOptions.attempts
        }

        if (producerOptions.attempts != null) {
            return producerOptions.attempts
        }

        return queueOptions.defaultAttempts
    }

    fun calculateVisibilityTimeout(
        queueOptions: QueueOptions,
        consumerOptions: ConsumerOptions,
        receiveOptions: ReceiveOptions
    ): Duration {
        if (receiveOptions.visibilityTimeout != null) {
            return receiveOptions.visibilityTimeout
        }

        if (consumerOptions.visibilityTimeout != null) {
            return consumerOptions.visibilityTimeout
        }

        return queueOptions.defaultVisibilityTimeout
    }

}
