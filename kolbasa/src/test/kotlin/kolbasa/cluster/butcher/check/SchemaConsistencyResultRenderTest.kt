package kolbasa.cluster.butcher.check

import kolbasa.butcher.ColumnChange
import kolbasa.butcher.ColumnShape
import kolbasa.butcher.NodeDelta
import kolbasa.butcher.SchemaConsistencyResult
import kolbasa.butcher.TableDivergence
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SchemaConsistencyResultRenderTest {

    @Test
    fun testClean() {
        assertEquals(
            "Schema consistency: all tables consistent across cluster",
            SchemaConsistencyResult(emptyMap()).render()
        )
    }

    @Test
    fun testColumnChange_RendersArrow() {
        val result = divergence(
            n3 to NodeDelta.ShapeDiff(
                addedColumns = emptySet(),
                removedColumns = emptySet(),
                changedColumns = setOf(
                    ColumnChange(
                        reference = ColumnShape("meta_foo", "INT", nullable = true, defaultExpression = null),
                        other = ColumnShape("meta_foo", "VARCHAR", nullable = true, defaultExpression = null),
                    )
                ),
                addedIndexes = emptySet(),
                removedIndexes = emptySet(),
                identityPresenceDiffers = false,
            )
        )

        val text = result.render()

        assertTrue(text.contains("~column meta_foo INT NULL -> VARCHAR NULL"), text)
    }

    @Test
    fun testDirty_ContainsExpectedFragments() {
        val result = divergence(
            n3 to NodeDelta.ShapeDiff(
                addedColumns = setOf(ColumnShape("meta_priority", "INT", nullable = true, defaultExpression = null)),
                removedColumns = emptySet(),
                changedColumns = emptySet(),
                addedIndexes = emptySet(),
                removedIndexes = emptySet(),
                identityPresenceDiffers = false,
            ),
            n4 to NodeDelta.Missing,
        )

        val text = result.render()

        assertTrue(text.contains("1 table(s) inconsistent"), text)
        assertTrue(text.contains("q_orders"), text)
        assertTrue(text.contains("[n1, n2]"), text)
        assertTrue(text.contains("n3: +column meta_priority"), text)
        assertTrue(text.contains("n4: missing"), text)
    }

    private fun divergence(vararg deltas: Pair<NodeId, NodeDelta>) = SchemaConsistencyResult(
        mapOf(
            "q_orders" to TableDivergence(
                table = "q_orders",
                referenceNodes = setOf(n1, n2),
                deltasByNode = deltas.toMap(),
            )
        )
    )

    private val n1 = NodeId("n1")
    private val n2 = NodeId("n2")
    private val n3 = NodeId("n3")
    private val n4 = NodeId("n4")

}
