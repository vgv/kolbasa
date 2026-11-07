package kolbasa.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BytesCounterTest {

    @Test
    fun testCounting() {
        val box = BytesCounter.Counting()

        assertEquals(0, box.get())
        box.addInt()
        assertEquals(0 + 4, box.get())
        box.addLong()
        assertEquals(0 + 4 + 8, box.get())
        box.addByteArray(byteArrayOf(1, 2, 3))
        assertEquals(0 + 4 + 8 + 3, box.get())
        box.addString("Привет") // 12 bytes in UTF-8
        assertEquals(0 + 4 + 8 + 3 + 12, box.get())
    }

    @Test
    fun testNone() {
        val box = BytesCounter.None

        // Nothing is counted
        box.addInt()
        box.addLong()
        box.addByteArray(byteArrayOf(1, 2, 3))
        box.addString("Привет") // 12 bytes in UTF-8
        assertEquals(0, box.get())
    }

}
