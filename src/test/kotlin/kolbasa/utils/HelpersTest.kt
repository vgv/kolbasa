package kolbasa.utils

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class HelpersTest {

    @Test
    fun testArrayToMap_IfNull() {
        assertNull(Helpers.arrayToMap(null))
    }

    @Test
    fun testArrayToMap_IfEven() {
        assertEquals(mapOf("1" to "2", "3" to "4"), Helpers.arrayToMap(arrayOf("1", "2", "3", "4")))
    }

    @Test
    fun testArrayToMap_IfOdd() {
        assertFailsWith<IllegalArgumentException> {
            Helpers.arrayToMap(arrayOf("1", "2", "3", "4", "5"))
        }
    }

}
