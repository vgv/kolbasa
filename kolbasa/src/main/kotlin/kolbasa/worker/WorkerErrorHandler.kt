package kolbasa.worker

/**
 * What to do when [BatchProcessor.process] throws inside a [QueueWorker], for example because the database is down.
 *
 * The worker goes on whatever the handler does: the thread waits [QueueWorkerOptions.errorPause] and tries again. So
 * this handler only reports the error. Errors of your [MessageHandler] or of the transform of a [QueuePipe] don't come
 * here, they go to the [ProcessErrorHandler].
 *
 * **A handler must not throw.** If it does, the worker thread ends, and the queue is processed by fewer threads. If
 * your handler calls code that can fail, for example your own logging or alerting, catch everything inside the
 * handler.
 *
 * The handler is called on the worker thread.
 *
 * By default, [SYSTEM_LOG] is used. Set another one with [QueueWorkerOptions.errorHandler].
 */
fun interface WorkerErrorHandler {

    /**
     * Called every time [BatchProcessor.process] throws. Must not throw, see [WorkerErrorHandler].
     */
    fun onWorkerError(error: WorkerError)

    companion object {

        /**
         * Writes a warning to the log. The default.
         *
         * The log is the JDK's [System.Logger] named `kolbasa.worker.WorkerErrorHandler`. By default, it
         * writes to `java.util.logging`. To send it to SLF4J or Log4j instead, add `slf4j-jdk-platform-logging` or
         * `log4j-jpl` to the application.
         */
        @JvmField
        val SYSTEM_LOG = WorkerErrorHandler { error ->
            WorkerErrorLogSupport.logger.log(
                System.Logger.Level.WARNING,
                "Can't process a batch from queue '${error.queue.name}', the worker will try again after a pause",
                error.cause
            )
        }
    }
}

private object WorkerErrorLogSupport {

    val logger: System.Logger = System.getLogger(WorkerErrorHandler::class.java.name)
}
