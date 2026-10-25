package kolbasa.worker

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Processes a queue in the background: starts [QueueWorkerOptions.threads] threads, and every thread calls
 * [QueueProcessor.process] in a loop.
 *
 * While the batches come full, a thread takes the next one at once. After a batch that wasn't full, the queue has
 * nothing more to give, so the thread waits [QueueWorkerOptions.successPause]. If [QueueProcessor.process] throws,
 * for example because the database is down, the error goes to the [WorkerErrorHandler] and the thread waits
 * [QueueWorkerOptions.errorPause], twice as long after every failure in a row, up to
 * [QueueWorkerOptions.maxErrorPause]. The worker never stops by itself.
 *
 * [stop] asks the threads to finish: a waiting thread stops at once, a busy one finishes its batch first. The
 * worker never interrupts its threads, so a handler that hangs keeps its thread forever. Give your handler its own
 * timeouts, for example on HTTP calls. Interrupting a worker thread from outside doesn't stop it either: only [stop]
 * does.
 *
 * A worker can be started only once. To start again, create a new one.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val processor = QueueProcessor(consumer, queue) { message ->
 *     process(message.data)
 *     HandleResult.DONE
 * }
 *
 * val worker = QueueWorker(processor, QueueWorkerOptions(threads = 4))
 * worker.start()
 * // ...
 * worker.stop(Duration.ofSeconds(30))
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var worker = new QueueWorker(processor, QueueWorkerOptions.builder().threads(4).build());
 * worker.start();
 * // ...
 * worker.stop(Duration.ofSeconds(30));
 * ```
 */
class QueueWorker @JvmOverloads constructor(
    /**
     * Processes one batch, called by every thread in a loop.
     */
    private val processor: QueueProcessor<*>,

    /**
     * How many threads, how long to wait, and how to make the threads.
     */
    private val options: QueueWorkerOptions = QueueWorkerOptions.DEFAULT
) : AutoCloseable {

    // Rings once, when the worker stops: a thread that waits for its next batch wakes up at once
    private val wakeUp = CountDownLatch(1)

    // Changed only under the lock, in start() and stop(), but read by the worker threads without it
    @Volatile
    private var state = State.NEW
    private var threads: List<Thread> = emptyList()

    /**
     * Starts the threads. Can be called only once.
     *
     * @throws IllegalStateException if the worker was already started or stopped
     */
    @Synchronized
    fun start() {
        check(state == State.NEW) {
            "The worker of queue '${processor.queue.name}' can be started only once, it is ${state.name.lowercase()}"
        }
        state = State.RUNNING

        val threadFactory = options.threadFactory ?: WorkerThreadFactory(processor.queue.name)
        threads = List(options.threads) { threadFactory.newThread(::loop) }
        threads.forEach(Thread::start)
    }

    /**
     * Asks the threads to finish and waits for them up to [timeout]. A waiting thread stops at once, a busy one
     * finishes its batch first. The threads are never interrupted.
     *
     * Can be called many times, and before [start] too. After it, the worker can't be started again.
     *
     * @return `true` if all threads have finished, `false` if some are still busy after [timeout]. They finish
     * their batches anyway and stop by themselves
     */
    @Synchronized
    fun stop(timeout: Duration): Boolean {
        state = State.STOPPED
        wakeUp.countDown()

        val deadline = System.nanoTime() + timeout.toNanos()
        for (thread in threads) {
            val left = deadline - System.nanoTime()
            if (left <= 0) {
                break
            }

            try {
                // At least one millisecond, because join(0) waits forever
                thread.join(TimeUnit.NANOSECONDS.toMillis(left).coerceAtLeast(1))
            } catch (e: InterruptedException) {
                // The thread that waits was interrupted: stop waiting, and keep the interruption for its owner
                Thread.currentThread().interrupt()
                break
            }
        }

        return threads.none(Thread::isAlive)
    }

    /**
     * The same as [stop] with [QueueWorkerOptions.stopTimeout], but doesn't tell whether all threads have finished.
     * If you need to know it, call [stop] instead. Threads that are still busy finish their batches anyway and stop
     * by themselves.
     */
    override fun close() {
        stop(options.stopTimeout)
    }

    private fun loop() {
        var batchesInRow = 0
        var failuresInRow = 0

        while (state == State.RUNNING) {
            val result = try {
                processor.process()
            } catch (e: Exception) {
                options.errorHandler.onWorkerError(WorkerError(processor.queue, e))
                batchesInRow = 0
                failuresInRow++
                pause(errorPause(failuresInRow))
                continue
            }

            failuresInRow = 0
            batchesInRow++
            if (!result.isBatchFull || batchesInRow >= options.maxBatchesBeforePause) {
                batchesInRow = 0
                pause(options.successPause)
            }
        }
    }

    // errorPause after the first failure, twice as long after every next one in a row, but not longer than
    // maxErrorPause. 30 doublings are far above any sane max, and they can't overflow
    internal fun errorPause(failuresInRow: Int): Duration {
        val doublings = (failuresInRow - 1).coerceIn(0, 30)   // failures 1, 2, 3, 4, 5... -> 0, 1, 2, 3, 4...
        val multiplier = 1L shl doublings                      // doublings 0, 1, 2, 3, 4... -> 1, 2, 4, 8, 16...
        return minOf(options.errorPause.multipliedBy(multiplier), options.maxErrorPause)   // 5s, 10s, 20s... 1m
    }

    // Waits up to 20% shorter or longer than delay, so the threads don't wake up together. stop() ends it at once
    private fun pause(delay: Duration) {
        val jitter = ThreadLocalRandom.current().nextDouble(1 - JITTER, 1 + JITTER)
        try {
            wakeUp.await((delay.toNanos() * jitter).toLong(), TimeUnit.NANOSECONDS)
        } catch (_: InterruptedException) {
            // Interrupting is not the way to stop a worker, stop() is. So it only ends this pause early
        }
    }

    private enum class State {
        NEW,
        RUNNING,
        STOPPED
    }

    private companion object {
        const val JITTER = 0.2
    }
}

/**
 * Ordinary (not daemon) threads named `kolbasa-worker-<queue>-<number>`, the default of
 * [QueueWorkerOptions.threadFactory].
 */
private class WorkerThreadFactory(private val queueName: String) : ThreadFactory {

    override fun newThread(r: Runnable): Thread {
        return Thread(r, "kolbasa-worker-$queueName-${counter.incrementAndGet()}")
    }

    private companion object {
        val counter = AtomicInteger(0)
    }
}
