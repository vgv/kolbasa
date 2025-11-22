package kolbasa.utils

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

internal class DaemonThreadFactory(private val name: String) : ThreadFactory {

    override fun newThread(r: Runnable): Thread {
        val thread = Thread(r, "${name}-${counter.incrementAndGet()}")
        thread.isDaemon = true
        return thread
    }

    private companion object {
        private val counter = AtomicInteger(0)
    }

}
