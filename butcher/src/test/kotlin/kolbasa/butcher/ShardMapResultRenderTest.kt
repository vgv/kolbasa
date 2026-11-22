package kolbasa.butcher

import kolbasa.cluster.admin.ShardCopyResult
import kolbasa.cluster.admin.ShardMapResult
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ShardMapResultRenderTest {

    private val n1 = NodeId("node-01")
    private val n2 = NodeId("node-02")
    private val n3 = NodeId("node-03")
    private val upToDate = ShardCopyResult(emptyList(), emptyList(), emptyList())

    @Test
    fun testClean() {
        val result = result(copies = mapOf(n1 to upToDate, n2 to upToDate, n3 to upToDate))

        assertEquals("Shard map: all 3 copies are the same", result.render().renderToText())
    }

    @Test
    fun testNoMap() {
        val text = result(
            copies = emptyMap(),
            nodesWithoutTable = setOf(n1, n2, n3),
            shardsWithoutRow = ShardId.SHARDS_RANGE.map { ShardId.of(it) }.toSet()
        ).render().renderToText()

        assertTrue(text.contains("Shard map: no shard map on any node yet"), text)
        assertTrue(text.contains("An application creates it when it starts"), text)
        // 1024 numbers would say nothing more
        assertTrue(!text.contains("1023"), text)
    }

    @Test
    fun testCopiesDiffer() {
        val text = result(
            copies = mapOf(n2 to ShardCopyResult(shards(1, 2), shards(3, 4, 5), shards(6)), n1 to upToDate),
            nodesWithoutTable = setOf(n3)
        ).render().renderToText()

        assertTrue(text.contains("2 of 3 copies differ"), text)
        assertEquals(listOf("node-01", "0", "0", "0"), rowOf("node-01", text))
        assertEquals(listOf("node-02", "2", "3", "1"), rowOf("node-02", text))
        assertEquals(listOf("node-03", "no table"), rowOf("node-03", text))
        assertTrue(text.contains("Applications bring the copies up to date"), text)
    }

    @Test
    fun testRowsThatCantBeRight() {
        val text = result(
            copies = mapOf(n1 to upToDate, n2 to upToDate, n3 to upToDate),
            shardsWithoutRow = shards(900, 8).toSet(),
            invalidShards = mapOf(n2 to listOf(6000, 5000)),
            unknownOwners = mapOf(NodeId("node-09") to shards(44, 17))
        ).render().renderToText()

        assertTrue(text.contains("2 shard(s) have no row on any node"), text)
        assertTrue(text.contains("2 row(s) of shards that don't exist"), text)
        assertTrue(text.contains("1 owner(s) not in the node list"), text)
        assertTrue(text.contains("No row on any node: 8,900"), text)
        assertTrue(text.contains("node-02: rows out of 0..1023: 5000,6000"), text)
        assertTrue(text.contains("Owner node-09 is not in the node list: shards 17,44"), text)
        // The copies are the same, so there is nothing to compare
        assertTrue(!text.contains("copies differ"), text)
        assertTrue(!text.contains("Outdated"), text)
    }

    // The cells of the table row of a node
    private fun rowOf(node: String, text: String): List<String> =
        text.lines().first { it.contains(node) }.split('\u2502').map { it.trim() }.filter { it.isNotEmpty() }

    private fun shards(vararg shards: Int): List<ShardId> = shards.map { ShardId.of(it) }

    private fun result(
        copies: Map<NodeId, ShardCopyResult>,
        nodesWithoutTable: Set<NodeId> = emptySet(),
        shardsWithoutRow: Set<ShardId> = emptySet(),
        invalidShards: Map<NodeId, List<Int>> = emptyMap(),
        unknownOwners: Map<NodeId, List<ShardId>> = emptyMap()
    ) = ShardMapResult(copies, nodesWithoutTable, shardsWithoutRow, invalidShards, unknownOwners)
}
