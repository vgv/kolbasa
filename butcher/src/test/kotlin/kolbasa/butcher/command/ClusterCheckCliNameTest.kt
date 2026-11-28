package kolbasa.butcher.command

import kolbasa.cluster.admin.ClusterCheck
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ClusterCheckCliNameTest {

    @Test
    fun testCheckNamesAreUnique() {
        val names = ClusterCheck.entries.map { check -> check.cliName() }

        assertEquals(names.size, names.toSet().size)
    }
}
