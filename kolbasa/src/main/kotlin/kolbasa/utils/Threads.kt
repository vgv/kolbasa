package kolbasa.utils

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong

/**
 * The threads kolbasa makes on its own: the default executors and the thread factories behind them.
 */
internal object Threads {

    /**
     * The default of [Kolbasa.asyncExecutor][kolbasa.Kolbasa.asyncExecutor].
     *
     * One thread, so the calls keep their order. It ends after a minute without calls, and the next call starts a new
     * one. A thread that never ends would keep forever what it got from the thread that created it, the context
     * classloader for one: under Spring devtools, the application from before a restart.
     */
    fun newAsyncExecutor(): Executor {
        val executor = ThreadPoolExecutor(
            1, 1,
            1, TimeUnit.MINUTES,
            LinkedBlockingQueue(),
            DaemonThreadFactory("kolbasa-async")
        )
        executor.allowCoreThreadTimeOut(true)
        return executor
    }

    /**
     * The default of [ClusterTopologyUpdaterOptions.executor][kolbasa.cluster.ClusterTopologyUpdaterOptions.executor].
     *
     * One thread for every updater in the JVM. It starts with the first scheduled update.
     */
    fun newClusterTopologyUpdaterExecutor(): ScheduledExecutorService {
        return Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory("kolbasa-cluster-topology-updater"))
    }
}

/**
 * Background (daemon) threads named `<name>-<number>`.
 */
internal class DaemonThreadFactory(private val name: String) : ThreadFactory {

    override fun newThread(r: Runnable): Thread {
        val thread = Thread(r, "${name}-${counter.incrementAndGet()}")
        thread.isDaemon = true
        return thread
    }

    private companion object {
        private val counter = AtomicLong(0)
    }

}

/**
 * Ordinary (not daemon) threads named `<name>-<number>`.
 */
internal class NonDaemonThreadFactory(private val name: String) : ThreadFactory {

    override fun newThread(r: Runnable): Thread {
        val thread = Thread(r, "${name}-${counter.incrementAndGet()}")
        thread.isDaemon = false
        return thread
    }

    private companion object {
        val counter = AtomicLong(0)
    }
}
