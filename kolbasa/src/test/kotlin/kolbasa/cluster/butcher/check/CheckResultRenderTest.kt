package kolbasa.cluster.butcher.check

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CheckResultRenderTest {

    @Test
    fun testNothingRequestedRendersNothing() {
        assertEquals("", CheckResult().render())
    }

    @Test
    fun testOnlyRequestedChecksAreRendered() {
        val rendered = CheckResult(migrationState = MigrationStateResult(emptyMap())).render()

        assertTrue(rendered.contains("Migration state"), rendered)
        assertTrue(!rendered.contains("Shard balance"), rendered)
    }

    @Test
    fun testEverySectionGetsItsOwnSeparator() {
        val rendered = CheckResult(
            shardBalance = ShardBalanceResult(emptyMap(), emptyMap()),
            migrationState = MigrationStateResult(emptyMap())
        ).render()

        assertEquals(2, rendered.lines().count { it.startsWith("=====") }, rendered)
        assertTrue(rendered.contains("Shard balance"), rendered)
        assertTrue(rendered.contains("Migration state"), rendered)
    }

}
