package kolbasa.butcher.cli

import kolbasa.butcher.OrphanTable
import kolbasa.butcher.OrphanTablesResult
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class OrphanTablesResultRenderTest {

    @Test
    fun testClean() {
        assertEquals("Orphan tables: no orphan queue tables", OrphanTablesResult(emptyMap()).render())
    }

    @Test
    fun testDirty_ContainsNodeAndTables() {
        val result = OrphanTablesResult(
            mapOf(
                NodeId("n1") to listOf(
                    OrphanTable("q_orders_dlq", "q_orders"),
                    OrphanTable("q_orders_arc", "q_orders"),
                ),
                NodeId("n2") to listOf(OrphanTable("q_payments_arc", "q_payments")),
            )
        )

        val text = result.render()

        assertTrue(text.contains("3 orphan queue(s) across 2 node(s)"), text)
        assertTrue(text.contains("q_orders_dlq"), text)
        assertTrue(text.contains("q_orders_arc"), text)
        assertTrue(text.contains("q_payments_arc"), text)
        assertTrue(text.contains("main queue q_orders missing"), text)
        assertTrue(text.contains("main queue q_payments missing"), text)
        // Nodes sorted by id: n1 before n2.
        val n1Pos = text.indexOf("n1:")
        val n2Pos = text.indexOf("n2:")
        assertTrue(n1Pos in 0 until n2Pos, "n1 group should come before n2 group:\n$text")
    }

}
