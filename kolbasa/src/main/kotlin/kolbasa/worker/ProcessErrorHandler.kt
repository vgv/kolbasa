package kolbasa.worker

/**
 * What to do when the [MessageHandler] throws.
 *
 * The message stays in the queue whatever the handler does: it comes back after the visibility timeout for another
 * attempt, or goes to the DLQ if that was the last one. So this handler only reports the error.
 *
 * **A handler must not throw.** If it does, [QueueProcessor.process] throws too, before it deletes the messages
 * that were handled well. They come back after the visibility timeout and are handled once more. If your handler
 * calls code that can fail, for example your own logging or alerting, catch everything inside the handler.
 *
 * The handler is called on the thread that calls [QueueProcessor.process], so it should be quick.
 *
 * By default, [SYSTEM_LOG] is used. Set another one with [QueueProcessorOptions.errorHandler].
 */
fun interface ProcessErrorHandler {

    /**
     * Called for every message that the [MessageHandler] failed to handle. Must not throw, see [ProcessErrorHandler].
     */
    fun onProcessError(error: ProcessError)

    companion object {

        /**
         * Writes to the log. The default.
         *
         * It is a warning, or an error on the last attempt, when the message is about to leave the queue for good.
         *
         * The log is the JDK's [System.Logger] named `kolbasa.worker.ProcessErrorHandler`. By default, it
         * writes to `java.util.logging`. To send it to SLF4J or Log4j instead, add `slf4j-jdk-platform-logging` or
         * `log4j-jpl` to the application.
         */
        @JvmField
        val SYSTEM_LOG = ProcessErrorHandler { error ->
            val level = if (error.message.isLastAttempt()) System.Logger.Level.ERROR else System.Logger.Level.WARNING
            val message = SystemLogSupport.logMessage(error)

            SystemLogSupport.logger.log(level, message, error.cause)
        }
    }
}

private object SystemLogSupport {

    val logger: System.Logger = System.getLogger(ProcessErrorHandler::class.java.name)

    fun logMessage(error: ProcessError): String {
        val attempts = if (error.message.isLastAttempt())
            "it was the last attempt"
        else
            "${error.message.remainingAttempts} attempts left"

        return "Can't process message ${error.message.id} from queue '${error.queue.name}', $attempts"
    }
}
