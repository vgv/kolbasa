package kolbasa.stats.task

import kolbasa.Kolbasa
import kolbasa.pg.Lock
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit

abstract class AbstractReschedulingTask : Runnable {

    abstract fun doWork()

    abstract fun reschedulingInterval(): Duration

    fun reschedule() {
        val interval = reschedulingInterval()
        Kolbasa.executor.schedule(this, interval.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun run() {
        try {
            doWork()
        } catch (e: Exception) {
            log.error("Unhandled exception in doWork() implementation", e)
        }

        reschedule()
    }

    private companion object {
        private val log = LoggerFactory.getLogger(AbstractReschedulingTask::class.qualifiedName)
    }

}
