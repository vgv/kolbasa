package kolbasa.utils

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class BoxesTest {

    @Test
    fun testIntBox() {
        val box = IntBox(42)

        assertEquals(42, box.get())
        assertEquals(42, box.getAndIncrement())
        assertEquals(43, box.get())
    }

    @Test
    fun testLongBox() {
        val box = LongBox(42)

        assertEquals(42, box.get())
        box.inc(5)
        assertEquals(42 + 5, box.get())
    }

}
