package kolbasa.worker.pipe

/**
 * What to do when the transform of a [QueuePipe] throws.
 *
 * The message stays in the source queue whatever the handler does: it comes back after the visibility timeout for
 * another attempt, or goes to the DLQ if that was the last one. So this handler only reports the error.
 *
 * **A handler must not throw.** If it does, [QueuePipe.process] throws too, before the batch is sent. No message of
 * the batch moves: all of them come back after the visibility timeout and are transformed once more. If your handler
 * calls code that can fail, for example your own logging or alerting, catch everything inside the handler.
 *
 * The handler is called on the thread that calls [QueuePipe.process], so it should be quick.
 *
 * By default, [SYSTEM_LOG] is used. Set another one with [QueuePipeOptions.errorHandler].
 */
fun interface TransformErrorHandler {

    /**
     * Called for every message that failed. Must not throw, see [TransformErrorHandler].
     */
    fun onTransformError(error: TransformError)

    companion object {

        /**
         * Writes to the log. The default.
         *
         * It is a warning, or an error on the last attempt, when the message is about to leave the source queue for
         * good.
         *
         * The log is the JDK's [System.Logger] named `kolbasa.worker.pipe.TransformErrorHandler`. By default, it
         * writes to `java.util.logging`. To send it to SLF4J or Log4j instead, add `slf4j-jdk-platform-logging` or
         * `log4j-jpl` to the application.
         */
        @JvmField
        val SYSTEM_LOG = TransformErrorHandler { error ->
            val level = if (error.message.isLastAttempt()) System.Logger.Level.ERROR else System.Logger.Level.WARNING
            val message = SystemLogSupport.logMessage(error)

            SystemLogSupport.logger.log(level, message, error.cause)
        }
    }
}

private object SystemLogSupport {

    val logger: System.Logger = System.getLogger(TransformErrorHandler::class.java.name)

    fun logMessage(error: TransformError): String {
        val attempts = if (error.message.isLastAttempt())
            "it was the last attempt"
        else
            "${error.message.remainingAttempts} attempts left"

        return "Can't transform message ${error.message.id} from queue '${error.queue.name}', $attempts"
    }
}
