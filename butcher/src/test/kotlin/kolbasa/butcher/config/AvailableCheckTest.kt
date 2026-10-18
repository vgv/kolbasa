package kolbasa.butcher.config

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class AvailableCheckTest {

    @Test
    fun testCheckNamesAreUnique() {
        val names = AvailableCheck.entries.map { it.checkName }
        Assertions.assertEquals(names.size, names.toSet().size)
    }
}
