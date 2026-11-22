package kolbasa.spring.boot

import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.worker.BatchProcessor
import kolbasa.worker.ProcessResult
import kolbasa.worker.QueueWorker
import kolbasa.worker.QueueWorkerOptions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ThreadFactory

class QueueWorkersTest {

    private val threads = RecordingThreadFactory()

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KolbasaAutoConfiguration::class.java))
        .withBean(QueueWorker::class.java, { QueueWorker(IdleProcessor(), QueueWorkerOptions(threadFactory = threads)) })

    @Test
    fun testWorkerBean_StartsAndStopsWithContext() {
        contextRunner.run { context ->
            assertThat(context).hasNotFailed()
            assertEquals(1, threads.created.size)
            assertTrue(threads.created.single().isAlive)
        }

        // The context has stopped, and so has the worker
        assertFalse(threads.created.single().isAlive)
    }

    @Test
    fun testAutoStartupOff_WorkerStaysStopped() {
        contextRunner
            .withPropertyValues("kolbasa.workers.auto-startup=false")
            .run { context ->
                assertThat(context).hasNotFailed()
                assertEquals(0, threads.created.size)
            }
    }

    // Never has anything to process, so the worker thread just waits
    private class IdleProcessor : BatchProcessor {
        override val queue = Queue.of("test", PredefinedDataTypes.String)

        override fun process() = ProcessResult(succeeded = 0, skipped = 0, failed = 0, requested = 1)
    }

    // Makes ordinary threads and remembers them, so the test sees whether the worker started
    private class RecordingThreadFactory : ThreadFactory {
        val created = CopyOnWriteArrayList<Thread>()

        override fun newThread(r: Runnable): Thread = Thread(r).also { created += it }
    }
}
