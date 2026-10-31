package kolbasa.worker

import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class QueueWorkerTest : AbstractPostgreSQLTest() {

    private val queue = Queue(
        name = "worker",
        databaseDataType = PredefinedDataTypes.String
    )

    // Every worker a test creates, so that a failed test doesn't leave its threads running
    private val workers = mutableListOf<QueueWorker>()

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @AfterEach
    fun after() {
        workers.forEach { it.stop(Duration.ofSeconds(10)) }
    }

    @Test
    fun testStart_ProcessesMessages() {
        val data = (1..25).map { "message_$it" }
        send(*data.toTypedArray())

        val handled = ConcurrentHashMap.newKeySet<String>()
        val processor = processor { message ->
            handled += message.data
            HandleResult.DONE
        }
        val worker = worker(processor, QueueWorkerOptions(threads = 2, successPause = Duration.ofMillis(100)))

        worker.start()
        awaitUntil { handled.size == data.size }

        // stop() waits for the last batches, so their messages are deleted by now
        assertTrue(worker.stop(Duration.ofSeconds(10)))
        assertEquals(data.toSet(), handled)
        assertEquals(0, messagesInQueue())
    }

    @Test
    fun testStart_AnyBatchProcessor() {
        // Not a QueueProcessor: no consumer, no messages, only a counter. Its batches are never full
        val calls = AtomicInteger()
        val processor = object : BatchProcessor {
            override val queue = this@QueueWorkerTest.queue

            override fun process(): ProcessResult {
                calls.incrementAndGet()
                return ProcessResult(succeeded = 0, skipped = 0, failed = 0, requested = 10)
            }
        }
        val worker = worker(processor, QueueWorkerOptions(successPause = Duration.ofMillis(100)))

        worker.start()
        awaitUntil { calls.get() >= 3 }
        assertTrue(worker.stop(Duration.ofSeconds(10)))
    }

    @Test
    fun testStop_WakesWaitingThreads() {
        // The queue is empty and the pause is one hour, so the threads wait, and stop() must wake them up at once
        val options = QueueWorkerOptions(threads = 3, successPause = Duration.ofHours(1))
        val worker = worker(processor { HandleResult.DONE }, options)
        worker.start()
        Thread.sleep(500)

        val start = System.nanoTime()
        assertTrue(worker.stop(Duration.ofSeconds(10)))
        val stopTime = Duration.ofNanos(System.nanoTime() - start)
        assertTrue(stopTime < Duration.ofSeconds(5), "Stopped in $stopTime")
    }

    @Test
    fun testStop_Timeout() {
        send("slow")

        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val processor = processor {
            started.countDown()
            release.await()
            HandleResult.DONE
        }
        val worker = worker(processor, QueueWorkerOptions(successPause = Duration.ofMillis(100)))
        worker.start()
        assertTrue(started.await(10, TimeUnit.SECONDS))

        // The handler is busy, so the worker can't stop in time
        assertFalse(worker.stop(Duration.ofMillis(200)))

        // Once the handler is done, the thread finishes its batch and stops by itself
        release.countDown()
        assertTrue(worker.stop(Duration.ofSeconds(10)))
        assertEquals(0, messagesInQueue())
    }

    @Test
    fun testMaxBatchesBeforePause() {
        send("1", "2", "3", "4", "5")

        // One message per batch, so every batch is full
        val handled = AtomicInteger()
        val processor = processor(QueueProcessorOptions(receiveLimit = 1)) {
            handled.incrementAndGet()
            HandleResult.DONE
        }

        // Two full batches in a row, then a pause of one hour
        val options = QueueWorkerOptions(maxBatchesBeforePause = 2, successPause = Duration.ofHours(1))
        val worker = worker(processor, options)
        worker.start()
        awaitUntil { handled.get() == 2 }

        // Give the thread a chance to take more, it must not
        Thread.sleep(500)
        assertTrue(worker.stop(Duration.ofSeconds(10)))
        assertEquals(2, handled.get())
        assertEquals(3, messagesInQueue())
    }

    @Test
    fun testProcessFails_WorkerKeepsGoing() {
        // No queue table, so every batch fails
        SchemaHelpers.deleteQueues(dataSource, queue)

        val handled = ConcurrentHashMap.newKeySet<String>()
        val processor = processor { message ->
            handled += message.data
            HandleResult.DONE
        }
        val errors = CopyOnWriteArrayList<WorkerError>()
        val options = QueueWorkerOptions(
            successPause = Duration.ofMillis(100),
            errorPause = Duration.ofMillis(100),
            maxErrorPause = Duration.ofMillis(200),
            errorHandler = { errors += it }
        )
        val worker = worker(processor, options)
        worker.start()

        // The failures go to the error handler
        awaitUntil { errors.isNotEmpty() }
        val error = errors.first()
        assertEquals(queue, error.queue)
        assertInstanceOf(SQLException::class.java, error.cause)

        // The table is back, and the worker picks the message up
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
        send("after the failure")
        awaitUntil { "after the failure" in handled }
        assertTrue(worker.stop(Duration.ofSeconds(10)))
    }

    @Test
    fun testInterrupt_DoesNotStopTheWorker() {
        val threads = ConcurrentHashMap.newKeySet<Thread>()
        val threadFactory = ThreadFactory { r -> Thread(r).also { threads += it } }

        val handled = ConcurrentHashMap.newKeySet<String>()
        val processor = processor { message ->
            handled += message.data
            HandleResult.DONE
        }

        // The queue is empty and the pause is one hour, so the thread waits
        val options = QueueWorkerOptions(successPause = Duration.ofHours(1), threadFactory = threadFactory)
        val worker = worker(processor, options)
        worker.start()
        Thread.sleep(500)

        // An interruption only ends the pause: the thread goes on and takes the new message
        send("after the interruption")
        threads.forEach(Thread::interrupt)
        awaitUntil { "after the interruption" in handled }
        assertTrue(threads.all(Thread::isAlive))

        assertTrue(worker.stop(Duration.ofSeconds(10)))
    }

    @Test
    fun testThreadNames() {
        send("bugaga")

        val threadNames = ConcurrentHashMap.newKeySet<String>()
        val processor = processor {
            threadNames += Thread.currentThread().name
            HandleResult.DONE
        }
        val worker = worker(processor, QueueWorkerOptions(successPause = Duration.ofMillis(100)))
        worker.start()
        awaitUntil { threadNames.isNotEmpty() }

        val name = threadNames.single()
        assertTrue(name.startsWith("kolbasa-worker-${queue.name}-"), "Thread name: $name")
    }

    @Test
    fun testStart_Twice_Fails() {
        val worker = worker(processor { HandleResult.DONE }, QueueWorkerOptions.DEFAULT)
        worker.start()

        assertThrows<IllegalStateException> { worker.start() }
    }

    @Test
    fun testStop_BeforeStart() {
        val worker = worker(processor { HandleResult.DONE }, QueueWorkerOptions.DEFAULT)

        // Nothing to wait for, and the worker can't be started after that
        assertTrue(worker.stop(Duration.ZERO))
        assertThrows<IllegalStateException> { worker.start() }
    }

    @Test
    fun testOptions_Checks() {
        assertThrows<IllegalArgumentException> { QueueWorkerOptions(threads = 0) }
        assertThrows<IllegalArgumentException> { QueueWorkerOptions(maxBatchesBeforePause = 0) }
        assertThrows<IllegalArgumentException> { QueueWorkerOptions(successPause = Duration.ZERO) }
        assertThrows<IllegalArgumentException> { QueueWorkerOptions(errorPause = Duration.ofSeconds(-1)) }
        assertThrows<IllegalArgumentException> {
            QueueWorkerOptions(errorPause = Duration.ofSeconds(10), maxErrorPause = Duration.ofSeconds(5))
        }
        assertThrows<IllegalArgumentException> { QueueWorkerOptions(stopTimeout = Duration.ofSeconds(-1)) }

        // Zero stop timeout means "don't wait", and the error pause may stay the same after failures
        QueueWorkerOptions(stopTimeout = Duration.ZERO)
        QueueWorkerOptions(errorPause = Duration.ofSeconds(5), maxErrorPause = Duration.ofSeconds(5))
    }

    @Test
    fun testErrorPause_GrowsUpToMax() {
        val options = QueueWorkerOptions(errorPause = Duration.ofSeconds(5), maxErrorPause = Duration.ofMinutes(1))
        val worker = worker(processor { HandleResult.DONE }, options)

        // Twice as long after every failure in a row
        assertEquals(Duration.ofSeconds(5), worker.errorPause(1))
        assertEquals(Duration.ofSeconds(10), worker.errorPause(2))
        assertEquals(Duration.ofSeconds(20), worker.errorPause(3))
        assertEquals(Duration.ofSeconds(40), worker.errorPause(4))

        // But never longer than the max, however many failures in a row
        assertEquals(Duration.ofMinutes(1), worker.errorPause(5))
        assertEquals(Duration.ofMinutes(1), worker.errorPause(Int.MAX_VALUE))
    }

    private fun processor(
        options: QueueProcessorOptions = QueueProcessorOptions.DEFAULT,
        handler: MessageHandler<String>
    ): QueueProcessor<String> {
        return QueueProcessor(queue, DatabaseConsumer(dataSource), options, handler)
    }

    private fun worker(processor: BatchProcessor, options: QueueWorkerOptions): QueueWorker {
        return QueueWorker(processor, options).also { workers += it }
    }

    private fun awaitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
        while (!condition()) {
            check(System.nanoTime() < deadline) { "The condition is still false after 10 seconds" }
            Thread.sleep(10)
        }
    }

    private fun send(vararg data: String) {
        DatabaseProducer(dataSource).send(queue, data.map { SendMessage(it) }).throwExceptionIfAny()
    }

    private fun messagesInQueue(): Int = dataSource.readInt("select count(*) from ${queue.dbTableName}")
}
