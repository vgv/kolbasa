package kolbasa.consumer

/**
 * What to do with a received message that can't be read: its data can't be deserialized, or its meta fields
 * can't be read.
 *
 * Such a message can't be returned from `receive()`. A handler that returns normally skips it, and `receive()`
 * returns the rest of the batch. The skipped message spends its attempt as usual and comes back after the
 * visibility timeout. When its attempts run out, it goes to the DLQ with its original data, or is deleted if the
 * queue has no DLQ.
 *
 * **A handler must not throw.** If it does, the whole `receive()` fails. Its transaction is rolled back, so the
 * message doesn't spend an attempt and comes first again on the next `receive()`. So one such message blocks the
 * queue, and nothing else can be received from it. If your handler calls code that can fail, for example your own
 * logging or alerting, catch everything inside the handler.
 *
 * The handler is called on the thread that calls `receive()`, inside its transaction, so it should be quick.
 *
 * By default, [SYSTEM_LOG] is used. Set another one with [ConsumerOptions.readErrorHandler].
 */
fun interface ReadErrorHandler {

    /**
     * Called for every message that can't be read. Must not throw, see [ReadErrorHandler].
     */
    fun onReadError(error: ReadError)

    companion object {

        /**
         * Skips the message silently.
         *
         * Nothing tells you about it, so the queue should have a DLQ. Without one, the message is lost for good
         * once its attempts run out.
         */
        @JvmField
        val SKIP = ReadErrorHandler { }

        /**
         * Writes to the log and skips the message. The default.
         *
         * It is a warning, or an error on the last attempt, when the message is about to leave the queue for good.
         *
         * The log is the JDK's [System.Logger] named `kolbasa.consumer.ReadErrorHandler`. By default, it writes to
         * `java.util.logging`. To send it to SLF4J or Log4j instead, add `slf4j-jdk-platform-logging` or
         * `log4j-jpl` to the application.
         */
        @JvmField
        val SYSTEM_LOG = ReadErrorHandler { error ->
            val level = if (error.isLastAttempt()) System.Logger.Level.ERROR else System.Logger.Level.WARNING
            val message = SystemLogSupport.logMessage(error)

            SystemLogSupport.logger.log(level, message, error.cause)
        }
    }
}

private object SystemLogSupport {

    val logger: System.Logger = System.getLogger(ReadErrorHandler::class.java.name)

    fun logMessage(error: ReadError): String {
        val node = if (error.nodeId.id.isNotEmpty())
            " on node '${error.nodeId.id}'"
        else
            ""

        val attempts = if (error.isLastAttempt())
            "it was the last attempt"
        else
            "${error.remainingAttempts} attempts left"

        return "Can't read message ${error.messageId} from queue '${error.queue.name}'$node, skipped, $attempts"
    }
}

