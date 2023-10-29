package kolbasa.utils

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class BytesCounterTest {

    @Test
    fun test() {
        val box = BytesCounter()

        assertEquals(0, box.get())
        box.inc(5)
        assertEquals(0 + 5, box.get())
        box.inc(42)
        assertEquals(0 + 5 + 42, box.get())
    }

}
