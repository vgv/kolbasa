package kolbasa.consumer.filter

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ColumnIndexTest {

    @Test
    fun nextIndex() {
        val columnIndex = ColumnIndex()

        assertEquals(1, columnIndex.nextIndex())
        assertEquals(2, columnIndex.nextIndex())
        assertEquals(3, columnIndex.nextIndex())
    }
}
