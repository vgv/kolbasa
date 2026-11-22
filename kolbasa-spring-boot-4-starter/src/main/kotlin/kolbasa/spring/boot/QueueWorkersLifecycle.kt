package kolbasa.spring.boot

import kolbasa.worker.QueueWorker
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.SmartLifecycle
import java.time.Duration

/**
 * Starts every `QueueWorker` bean once the application is up, and stops them first when it stops.
 *
 * The workers are beans of yours: you build them, the starter starts and stops them. Don't call `start()` in the bean
 * method: a worker starts only once, so the starter's `start()` would fail the application.
 *
 * It runs in the last phase: the workers start after everything else, the web server too, and stop before anything
 * else.
 */
internal class QueueWorkersLifecycle(private val workers: ObjectProvider<QueueWorker>) : SmartLifecycle {

    @Volatile
    private var running = false

    override fun start() {
        workers.forEach { it.start() }
        running = true
    }

    override fun stop() {
        // Tell every worker to finish first, then wait for each. So the workers finish their batches at the same time,
        // not one after another
        workers.forEach { it.stop(Duration.ZERO) }
        workers.forEach { it.close() }
        running = false
    }

    override fun isRunning(): Boolean = running
}
