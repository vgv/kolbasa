package kolbasa.stats

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals


class SlidingWindowTest {

    @Test
    fun testSlidingWindow() {
        val window = SlidingWindow(Measure.SEND_1M)
        window.inc(1)
        window.inc(2)
        window.inc(3)

        val firstDump = window.dumpAndReset()
        val secondDump = window.dumpAndReset()

        assertEquals(6, firstDump.data.values.sum())
        assertEquals(Measure.SEND_1M, firstDump.measure)

        assertEquals(0, secondDump.data.values.sum())
        assertEquals(Measure.SEND_1M, secondDump.measure)
    }

}
