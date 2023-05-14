package kolbasa.task

import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory

internal val DefaultExecutor = Executors.newScheduledThreadPool(1, DefaultDaemonFactory)

private object DefaultDaemonFactory : ThreadFactory {
    override fun newThread(r: Runnable): Thread {
        val thread = Thread(r, "kolbasa-default-executor")
        thread.isDaemon = true
        thread.uncaughtExceptionHandler = UncaughtExceptionHandler
        return thread
    }
}

private object UncaughtExceptionHandler : Thread.UncaughtExceptionHandler {

    private val log = LoggerFactory.getLogger(UncaughtExceptionHandler::class.qualifiedName)

    override fun uncaughtException(thread: Thread, ex: Throwable) {
        log.error("Uncaught exception", ex)
    }
}

