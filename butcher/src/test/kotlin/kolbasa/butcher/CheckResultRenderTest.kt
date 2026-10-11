package kolbasa.butcher

import kolbasa.cluster.admin.CheckResult
import kolbasa.cluster.admin.MigrationStateResult
import kolbasa.cluster.admin.ShardBalanceResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CheckResultRenderTest {

    @Test
    fun testNothingRequestedRendersNothing() {
        assertEquals("", CheckResult().render().renderToText())
    }

    @Test
    fun testOnlyRequestedChecksAreRendered() {
        val rendered = CheckResult(migrationState = MigrationStateResult(emptyMap())).render().renderToText()

        assertTrue(rendered.contains("Migration state"), rendered)
        assertTrue(!rendered.contains("Shard balance"), rendered)
    }

    @Test
    fun testEverySectionGetsItsOwnSeparator() {
        val rendered = CheckResult(
            shardBalance = ShardBalanceResult(emptyMap(), emptyMap()),
            migrationState = MigrationStateResult(emptyMap())
        ).render().renderToText()

        // One horizontal rule between the two sections, and none around them
        assertEquals(1, rendered.lines().count { it.isNotEmpty() && it.all { ch -> ch == '\u2500' } }, rendered)
        assertTrue(rendered.contains("Shard balance"), rendered)
        assertTrue(rendered.contains("Migration state"), rendered)
    }

}
