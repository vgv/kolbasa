package kolbasa.cluster.butcher.check

import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class OrphanTablesTest {

    @Test
    fun testEmptyCluster_IsClean() {
        val result = OrphanTables(emptyMap()).compute()

        assertTrue(result.isClean)
        assertEquals(0, result.totalOrphans)
        assertTrue(result.orphansByNode.isEmpty())
    }

    @Test
    fun testSingleNode_NoOrphans_IsClean() {
        val n1 = NodeId("n1")
        val tables = mapOf(n1 to setOf("q_orders", "q_orders_dlq", "q_orders_arc"))

        val result = OrphanTables(tables).compute()

        assertTrue(result.isClean)
        assertEquals(0, result.totalOrphans)
    }

    @Test
    fun testSingleNode_DlqOrphan() {
        val n1 = NodeId("n1")
        val tables = mapOf(n1 to setOf("q_orders_dlq"))

        val result = OrphanTables(tables).compute()

        assertFalse(result.isClean)
        assertEquals(1, result.totalOrphans)
        assertEquals(
            listOf(OrphanTable("q_orders_dlq", "q_orders")),
            result.orphansByNode[n1]
        )
    }

    @Test
    fun testSingleNode_ArchiveOrphan() {
        val n1 = NodeId("n1")
        val tables = mapOf(n1 to setOf("q_orders_arc"))

        val result = OrphanTables(tables).compute()

        assertFalse(result.isClean)
        assertEquals(1, result.totalOrphans)
        assertEquals(
            listOf(OrphanTable("q_orders_arc", "q_orders")),
            result.orphansByNode[n1]
        )
    }

    @Test
    fun testSingleNode_BothCompanionsOrphaned() {
        val n1 = NodeId("n1")
        val tables = mapOf(n1 to setOf("q_orders_dlq", "q_orders_arc"))

        val result = OrphanTables(tables).compute()

        assertEquals(2, result.totalOrphans)
        assertEquals(
            listOf(
                OrphanTable("q_orders_arc", "q_orders"),
                OrphanTable("q_orders_dlq", "q_orders"),
            ),
            result.orphansByNode[n1]
        )
    }

    @Test
    fun testMainPresent_CompanionsPresent_NoOrphans() {
        val n1 = NodeId("n1")
        val tables = mapOf(n1 to setOf("q_orders", "q_orders_dlq"))

        val result = OrphanTables(tables).compute()

        assertTrue(result.isClean)
    }

    @Test
    fun testOnlyMainQueue_NoOrphans() {
        val n1 = NodeId("n1")
        val tables = mapOf(n1 to setOf("q_orders"))

        val result = OrphanTables(tables).compute()

        assertTrue(result.isClean)
    }

    @Test
    fun testMultipleNodes_OnlyOneHasOrphans() {
        val n1 = NodeId("n1")
        val n2 = NodeId("n2")
        val tables = mapOf(
            n1 to setOf("q_orders", "q_orders_dlq"),
            n2 to setOf("q_payments_dlq"),
        )

        val result = OrphanTables(tables).compute()

        assertEquals(1, result.totalOrphans)
        assertEquals(setOf(n2), result.orphansByNode.keys)
        assertEquals(
            listOf(OrphanTable("q_payments_dlq", "q_payments")),
            result.orphansByNode[n2]
        )
    }

    @Test
    fun testMultipleNodes_DifferentOrphansEach() {
        val n1 = NodeId("n1")
        val n2 = NodeId("n2")
        val tables = mapOf(
            n1 to setOf("q_orders_dlq"),
            n2 to setOf("q_payments_arc"),
        )

        val result = OrphanTables(tables).compute()

        assertEquals(2, result.totalOrphans)
        assertEquals(
            listOf(OrphanTable("q_orders_dlq", "q_orders")),
            result.orphansByNode[n1]
        )
        assertEquals(
            listOf(OrphanTable("q_payments_arc", "q_payments")),
            result.orphansByNode[n2]
        )
    }

    @Test
    fun testNonKolbasaTablesIgnored() {
        val n1 = NodeId("n1")
        val tables = mapOf(
            n1 to setOf("other_dlq", "random_arc", "users", "q_orders", "q_orders_dlq"),
        )

        val result = OrphanTables(tables).compute()

        assertTrue(result.isClean)
    }

    @Test
    fun testDegenerateNamesIgnored() {
        val n1 = NodeId("n1")
        val tables = mapOf(n1 to setOf("q__dlq", "q__arc"))

        val result = OrphanTables(tables).compute()

        assertTrue(result.isClean)
    }

    @Test
    fun testToString_Clean() {
        val result = OrphanTables(emptyMap()).compute()

        assertEquals("Orphan tables: no orphan companion tables", result.toString())
    }

    @Test
    fun testToString_Dirty_ContainsNodeAndTables() {
        val n1 = NodeId("n1")
        val n2 = NodeId("n2")
        val tables = mapOf(
            n1 to setOf("q_orders_dlq", "q_orders_arc"),
            n2 to setOf("q_payments_arc"),
        )

        val text = OrphanTables(tables).compute().toString()

        assertTrue(text.contains("3 orphan companion(s) across 2 node(s)"), text)
        assertTrue(text.contains("n1"), text)
        assertTrue(text.contains("n2"), text)
        assertTrue(text.contains("q_orders_dlq"), text)
        assertTrue(text.contains("q_orders_arc"), text)
        assertTrue(text.contains("q_payments_arc"), text)
        assertTrue(text.contains("main q_orders missing"), text)
        assertTrue(text.contains("main q_payments missing"), text)
        // Nodes sorted by id: n1 before n2.
        val n1Pos = text.indexOf("n1:")
        val n2Pos = text.indexOf("n2:")
        assertTrue(n1Pos in 0 until n2Pos, "n1 group should come before n2 group:\n$text")
    }
}
