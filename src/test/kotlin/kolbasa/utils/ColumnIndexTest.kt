package kolbasa.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ColumnIndexTest {

    @Test
    fun nextIndex() {
        val columnIndex = ColumnIndex()

        assertEquals(1, columnIndex.nextIndex())
        assertEquals(2, columnIndex.nextIndex())
        assertEquals(3, columnIndex.nextIndex())
    }
}
