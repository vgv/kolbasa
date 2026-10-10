package kolbasa.inspector

import kolbasa.inspector.Messages.Companion.merge
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class MessagesTest {

    @Test
    fun testTotal() {
        val messages = Messages(1, 2, 3, 4, 5)
        assertEquals(15, messages.total())
    }

    @Test
    fun testTotal_AllZeros() {
        assertEquals(0, Messages.DEFAULT.total())
    }

    @Test
    fun testMerge_Multiple() {
        val list = listOf(
            Messages(1, 2, 3, 4, 5),
            Messages(10, 20, 30, 40, 50),
            Messages(100, 200, 300, 400, 500)
        )

        val merged = list.merge()
        assertEquals(Messages(111, 222, 333, 444, 555), merged)
    }

    @Test
    fun testMerge_Empty() {
        val merged = emptyList<Messages>().merge()
        assertEquals(Messages.DEFAULT, merged)
    }

    @Test
    fun testMerge_Single() {
        val single = Messages(1, 2, 3, 4, 5)
        val merged = listOf(single).merge()
        assertEquals(single, merged)
    }

}
