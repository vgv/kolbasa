package kolbasa.utils

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.ThreadFactory

class ThreadsTest {

    @Test
    fun testDaemonThreadFactory() {
        // Made from an ordinary thread, so the flag can only come from the factory
        val created = newThreadFrom(daemon = false, DaemonThreadFactory("daemon_factory"))

        assertTrue(created.isDaemon)
        // The number is not known: all factories count threads together
        assertTrue(created.name.matches(Regex("daemon_factory-\\d+")), "Thread name: ${created.name}")
    }

    @Test
    fun testNonDaemonThreadFactory() {
        // Made from a daemon thread, so the flag can only come from the factory
        val created = newThreadFrom(daemon = true, NonDaemonThreadFactory("non_daemon_factory"))

        assertFalse(created.isDaemon)
        // The number is not known: all factories count threads together, and the workers of other tests count too
        assertTrue(created.name.matches(Regex("non_daemon_factory-\\d+")), "Thread name: ${created.name}")
    }

    // A new thread takes the daemon flag of the thread that creates it, unless the factory sets it. So the factory
    // makes its thread on a creator thread with the opposite flag
    private fun newThreadFrom(daemon: Boolean, factory: ThreadFactory): Thread {
        var thread: Thread? = null
        val creator = Thread { thread = factory.newThread(mockk()) }
        creator.isDaemon = daemon
        creator.start()
        creator.join()
        return checkNotNull(thread)
    }

}
