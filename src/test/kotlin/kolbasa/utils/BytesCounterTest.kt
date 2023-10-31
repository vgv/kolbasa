package kolbasa.utils

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class BytesCounterTest {

    @Test
    fun testPreciseCalculation() {
        val box = BytesCounter(true)

        assertEquals(0, box.get())
        box.addInt()
        assertEquals(0 + 4, box.get())
        box.addLong()
        assertEquals(0 + 4 + 8, box.get())
        box.addByteArray(byteArrayOf(1, 2, 3))
        assertEquals(0 + 4 + 8 + 3, box.get())
        box.addString("Привет")
        assertEquals(0 + 4 + 8 + 3 + 12, box.get())
    }

    @Test
    fun testNotPreciseCalculation() {
        val box = BytesCounter(false)

        assertEquals(0, box.get())
        box.addInt()
        assertEquals(0 + 4, box.get())
        box.addLong()
        assertEquals(0 + 4 + 8, box.get())
        box.addByteArray(byteArrayOf(1, 2, 3))
        assertEquals(0 + 4 + 8 + 3, box.get())
        box.addString("Привет")
        assertEquals(0 + 4 + 8 + 3 + 6, box.get())
    }

}
