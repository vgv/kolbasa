package kolbasa.cluster.butcher.config

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull

class AvailableCheckTest {

    @Test
    fun testAll_ContainsEveryEntry() {
        assertEquals(AvailableCheck.entries.toSet(), AvailableCheck.all)
    }

    @Test
    fun testFromCheckName_KnownName() {
        assertEquals(
            AvailableCheck.CHECK_SHARD_BALANCE,
            AvailableCheck.fromCheckName("shard-balance")
        )

        assertEquals(
            AvailableCheck.CHECK_SCHEMA_CONSISTENCY,
            AvailableCheck.fromCheckName("schema-consistency")
        )

        assertEquals(
            AvailableCheck.FIND_ORPHAN_TABLES,
            AvailableCheck.fromCheckName("find-orphans")
        )

        assertEquals(
            AvailableCheck.CHECK_MIGRATION_STATE,
            AvailableCheck.fromCheckName("migration-state")
        )
    }

    @Test
    fun testFromCheckName_Unknown_And_CASE_SENSITIVE_ReturnsNull() {
        assertNull(AvailableCheck.fromCheckName("does-not-exist"))
        assertNull(AvailableCheck.fromCheckName(""))
        assertNull(AvailableCheck.fromCheckName("all"))
        assertNull(AvailableCheck.fromCheckName("SHARD-BALANCE"))
    }

    @Test
    fun testCheckNamesAreUnique() {
        val names = AvailableCheck.entries.map { it.checkName }
        Assertions.assertEquals(names.size, names.toSet().size)
    }
}
