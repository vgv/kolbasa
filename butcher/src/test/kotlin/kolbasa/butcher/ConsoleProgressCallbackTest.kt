package kolbasa.butcher

import com.github.ajalt.mordant.rendering.AnsiLevel
import com.github.ajalt.mordant.terminal.Terminal
import com.github.ajalt.mordant.terminal.TerminalRecorder
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import javax.sql.DataSource

/**
 * How migration progress reads when nobody is watching a terminal
 *
 * This is the piped case – `butcher move-data | grep …`, a CI log – and it is the one worth pinning down,
 * because mordant renders an animation only on an interactive terminal: without the plain-line fallback a
 * redirected run would print no progress at all.
 */
class ConsoleProgressCallbackTest {

    private val recorder = TerminalRecorder(ansiLevel = AnsiLevel.NONE, width = 120)
    private val terminal = Terminal(terminalInterface = recorder)
    private val callback = ConsoleProgressCallback(terminal)

    private val from = mockk<DataSource>(relaxed = true)
    private val to = mockk<DataSource>(relaxed = true)

    @Test
    fun testNonInteractiveTerminalIsNotInteractive() {
        // The premise of everything below: with this false, mordant draws no animation at all
        assertEquals(false, terminal.terminalInfo.interactive)
    }

    @Test
    fun testBatchLineCarriesAllThreeNumbers() {
        callback.moveNextBatch("q_orders", from, to, 1_000, 10_000L, 10_200_200L)

        // Grouped digits: the interesting part of 10200200 is how many digits it has
        assertTrue(
            recorder.stdout().contains("q_orders: moved 1,000 rows, table: 10,000 rows, total: 10,200,200 rows"),
            recorder.stdout()
        )
    }

    @Test
    fun testProgressCarriesNoAnsiCodes() {
        callback.moveNextBatch("q_orders", from, to, 1_000, 1_000L, 1_000L)

        // Nothing here may reach grep as an escape sequence or as a line that never ends
        assertTrue(!recorder.stdout().contains('\u001B'), recorder.stdout())
        assertTrue(!recorder.stdout().contains('\r'), recorder.stdout())
        assertTrue(recorder.stdout().endsWith("\n"), recorder.stdout())
    }

    @Test
    fun testEveryBatchIsPrintedWhenThereIsNoTerminalToRewrite() {
        repeat(50) { callback.moveNextBatch("q_orders", from, to, 1_000, (it + 1) * 1_000L, (it + 1) * 1_000L) }

        // A redirected run gets one line per batch. That is 10 000 lines for a 10M-row queue, which is the
        // reason this used to be throttled - see the class KDoc, which still describes a throttle
        assertEquals(50, recorder.stdout().lines().count { it.startsWith("q_orders:") }, recorder.stdout())
    }

    @Test
    fun testEveryTableGetsItsOwnSummary() {
        callback.moveNextBatch("q_orders", from, to, 1_000, 1_000L, 1_000L)
        callback.moveEnd("q_orders", from, to, 10_000L)
        callback.moveNextBatch("q_payments", from, to, 1_000, 1_000L, 11_000L)
        callback.moveEnd("q_payments", from, to, 10_000L)

        val summaries = recorder.stdout().lines().filter { it.contains("finished, migrated rows") }
        assertEquals(2, summaries.size, recorder.stdout())
        summaries.forEach { assertTrue(it.contains("10,000"), it) }
    }

}
