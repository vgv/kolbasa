package kolbasa.worker

import kolbasa.queue.Checks
import java.time.Duration
import java.util.concurrent.ThreadFactory

/**
 * Options of a [QueueWorker].
 *
 * The defaults are careful: one thread, a pause of 5 seconds when the queue has nothing to give. Raise them when you
 * know you need more.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val options = QueueWorkerOptions(threads = 4, successPause = Duration.ofSeconds(1))
 *
 * val worker = QueueWorker(processor, options)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var options = QueueWorkerOptions.builder()
 *     .threads(4)
 *     .successPause(Duration.ofSeconds(1))
 *     .build();
 *
 * var worker = new QueueWorker(processor, options);
 * ```
 */
data class QueueWorkerOptions(
    /**
     * How many threads process the queue at the same time. Every thread calls [BatchProcessor.process] in its own
     * loop. The default is 1.
     */
    val threads: Int = 1,

    /**
     * How many full batches one thread processes in a row before it makes a [successPause]. By default, there is no
     * limit: while the batches come full, the thread takes the next one at once.
     *
     * Set it to slow the worker down, for example to protect a system that your handler calls. Then one thread
     * handles about `receiveLimit × maxBatchesBeforePause` messages, then makes a pause.
     */
    val maxBatchesBeforePause: Int = Int.MAX_VALUE,

    /**
     * How long a thread waits after a successful batch, the opposite of [errorPause]. Not after every one: only
     * after a batch that wasn't full, or after [maxBatchesBeforePause] full batches in a row. After a full batch the
     * thread takes the next one at once. A batch that isn't full means the queue has nothing more to give right now.
     * The default is 5 seconds.
     *
     * Every thread makes one query when the queue is empty, so an empty queue costs about `threads / successPause`
     * queries per second. The real pause is up to 20% shorter or longer, so the threads don't wake up together.
     */
    val successPause: Duration = DEFAULT_SUCCESS_PAUSE,

    /**
     * How long a thread waits after [BatchProcessor.process] failed, for example because the database is down. The
     * default is 5 seconds.
     *
     * After every failure in a row the pause is twice as long, up to [maxErrorPause]: a short failure is retried
     * soon, a long one doesn't flood the database and the log. The first batch that works brings the pause back to
     * this value. The real pause is up to 20% shorter or longer, so the threads don't come back together.
     */
    val errorPause: Duration = DEFAULT_ERROR_PAUSE,

    /**
     * The longest pause after failures in a row, see [errorPause]. The default is 1 minute.
     */
    val maxErrorPause: Duration = DEFAULT_MAX_ERROR_PAUSE,

    /**
     * What to do when [BatchProcessor.process] fails. By default, the error goes to the log, see
     * [WorkerErrorHandler.SYSTEM_LOG].
     */
    val errorHandler: WorkerErrorHandler = WorkerErrorHandler.SYSTEM_LOG,

    /**
     * How long [QueueWorker.close] waits for the threads to finish their batches. The default is 30 seconds.
     */
    val stopTimeout: Duration = DEFAULT_STOP_TIMEOUT,

    /**
     * Makes the threads. By default, the worker makes ordinary (not daemon) threads named
     * `kolbasa-worker-<queue>-<number>`. Pass your own, for example, to get daemon or virtual threads.
     */
    val threadFactory: ThreadFactory? = null
) {

    init {
        Checks.checkWorkerThreads(threads)
        Checks.checkMaxBatchesBeforePause(maxBatchesBeforePause)
        Checks.checkWorkerPause("Success pause", successPause)
        Checks.checkWorkerPause("Error pause", errorPause)
        Checks.checkMaxErrorPause(errorPause, maxErrorPause)
        Checks.checkWorkerStopTimeout(stopTimeout)
    }

    /**
     * Builder for flexible [QueueWorkerOptions] creation, when only some of the properties need to be set.
     */
    class Builder {
        private var threads: Int = 1
        private var maxBatchesBeforePause: Int = Int.MAX_VALUE
        private var successPause: Duration = DEFAULT_SUCCESS_PAUSE
        private var errorPause: Duration = DEFAULT_ERROR_PAUSE
        private var maxErrorPause: Duration = DEFAULT_MAX_ERROR_PAUSE
        private var errorHandler: WorkerErrorHandler = WorkerErrorHandler.SYSTEM_LOG
        private var stopTimeout: Duration = DEFAULT_STOP_TIMEOUT
        private var threadFactory: ThreadFactory? = null

        /**
         * Sets [QueueWorkerOptions.threads] – how many threads process the queue at the same time.
         */
        fun threads(threads: Int) = apply { this.threads = threads }

        /**
         * Sets [QueueWorkerOptions.maxBatchesBeforePause] – how many full batches a thread processes before a pause.
         */
        fun maxBatchesBeforePause(maxBatchesBeforePause: Int) = apply {
            this.maxBatchesBeforePause = maxBatchesBeforePause
        }

        /**
         * Sets [QueueWorkerOptions.successPause] – the pause after a successful batch that wasn't full.
         */
        fun successPause(successPause: Duration) = apply { this.successPause = successPause }

        /**
         * Sets [QueueWorkerOptions.errorPause] – the pause after the first failed batch.
         */
        fun errorPause(errorPause: Duration) = apply { this.errorPause = errorPause }

        /**
         * Sets [QueueWorkerOptions.maxErrorPause] – the longest pause after failures in a row.
         */
        fun maxErrorPause(maxErrorPause: Duration) = apply { this.maxErrorPause = maxErrorPause }

        /**
         * Sets [QueueWorkerOptions.errorHandler] – what to do when a batch fails.
         */
        fun errorHandler(errorHandler: WorkerErrorHandler) = apply { this.errorHandler = errorHandler }

        /**
         * Sets [QueueWorkerOptions.stopTimeout] – how long `close()` waits for the threads.
         */
        fun stopTimeout(stopTimeout: Duration) = apply { this.stopTimeout = stopTimeout }

        /**
         * Sets [QueueWorkerOptions.threadFactory] – what makes the threads.
         */
        fun threadFactory(threadFactory: ThreadFactory) = apply { this.threadFactory = threadFactory }

        /**
         * Creates a new [QueueWorkerOptions] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = QueueWorkerOptions(
            threads = threads,
            maxBatchesBeforePause = maxBatchesBeforePause,
            successPause = successPause,
            errorPause = errorPause,
            maxErrorPause = maxErrorPause,
            errorHandler = errorHandler,
            stopTimeout = stopTimeout,
            threadFactory = threadFactory
        )
    }

    companion object {

        /**
         * The pause after a successful batch, unless [successPause] says otherwise.
         */
        @JvmField
        val DEFAULT_SUCCESS_PAUSE: Duration = Duration.ofSeconds(5)

        /**
         * The pause after the first failed batch, unless [errorPause] says otherwise.
         */
        @JvmField
        val DEFAULT_ERROR_PAUSE: Duration = Duration.ofSeconds(5)

        /**
         * The longest pause after failures in a row, unless [maxErrorPause] says otherwise.
         */
        @JvmField
        val DEFAULT_MAX_ERROR_PAUSE: Duration = Duration.ofMinutes(1)

        /**
         * How long `close()` waits for the threads, unless [stopTimeout] says otherwise.
         */
        @JvmField
        val DEFAULT_STOP_TIMEOUT: Duration = Duration.ofSeconds(30)

        /**
         * Default options: one thread, no limit on full batches in a row, a pause of 5 seconds, error pauses from
         * 5 seconds up to 1 minute, 30 seconds to stop.
         */
        @JvmField
        val DEFAULT = QueueWorkerOptions()

        @JvmStatic
        fun builder() = Builder()
    }
}
