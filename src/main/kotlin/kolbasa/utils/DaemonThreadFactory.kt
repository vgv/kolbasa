package kolbasa.utils

import java.util.concurrent.ThreadFactory

internal class DaemonThreadFactory(private val name: String) : ThreadFactory {

    override fun newThread(r: Runnable): Thread {
        val thread = Thread(r, name)
        thread.isDaemon = true
        return thread
    }

}
