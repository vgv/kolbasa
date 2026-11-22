package kolbasa.cluster.admin.check

import kolbasa.cluster.Shard
import kolbasa.cluster.admin.ShardCopyResult
import kolbasa.cluster.admin.ShardMapResult
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

class ShardMapStateTest {

    private val n1 = NodeId("node-01")
    private val n2 = NodeId("node-02")
    private val n3 = NodeId("node-03")
    private val upToDate = ShardCopyResult(emptyList(), emptyList(), emptyList())

    // The newest rows: every shard is on the first node
    private val map = ShardId.SHARDS_RANGE.associate { ShardId.of(it) to stableShard(it, n1) }

    @Test
    fun testSameCopies_Clean() {
        val result = check(copies = mapOf(n1 to map, n2 to map, n3 to map))

        assertTrue(result.isClean)
        assertTrue(result.isComplete)
        assertFalse(result.isMissing)
        assertEquals(mapOf(n1 to upToDate, n2 to upToDate, n3 to upToDate), result.copies)
    }

    @Test
    fun testCopiesDiffer() {
        val newest = map + (ShardId.of(5) to stableShard(5, n2, NEWER))
        val lagging = map - ShardId.of(3) - ShardId.of(4)
        val conflicting = newest + (ShardId.of(7) to stableShard(7, n3))

        val result = check(merged = newest, copies = mapOf(n1 to newest, n2 to lagging, n3 to conflicting))

        assertEquals(upToDate, result.copies[n1])
        assertEquals(
            ShardCopyResult(missing = shards(3, 4), outdated = shards(5), conflicting = emptyList()),
            result.copies[n2]
        )
        assertEquals(
            ShardCopyResult(missing = emptyList(), outdated = emptyList(), conflicting = shards(7)),
            result.copies[n3]
        )

        // The map itself is whole, only its copies differ
        assertTrue(result.isComplete)
        assertFalse(result.isClean)
    }

    @Test
    fun testNodeWithoutTable() {
        val result = check(copies = mapOf(n1 to map, n2 to map))

        assertEquals(setOf(n3), result.nodesWithoutTable)
        assertFalse(result.isComplete)
        assertFalse(result.isClean)
    }

    @Test
    fun testShardsNoNodeHas() {
        val partial = map - ShardId.of(0) - ShardId.of(1023)

        val result = check(merged = partial, copies = mapOf(n1 to partial, n2 to partial, n3 to partial))

        assertEquals(shards(0, 1023).toSet(), result.shardsWithoutRow)
        assertFalse(result.isComplete)
        assertFalse(result.isMissing)
        // The copies have everything there is
        assertEquals(upToDate, result.copies[n1])
    }

    @Test
    fun testNoMapAnywhere() {
        val noTables = check(merged = emptyMap(), copies = emptyMap())
        assertTrue(noTables.isMissing)
        assertEquals(setOf(n1, n2, n3), noTables.nodesWithoutTable)

        val emptyTables = check(merged = emptyMap(), copies = mapOf(n1 to emptyMap(), n2 to emptyMap(), n3 to emptyMap()))
        assertTrue(emptyTables.isMissing)
        assertFalse(emptyTables.isClean)
    }

    @Test
    fun testInvalidShards() {
        val result = check(
            copies = mapOf(n1 to map, n2 to map, n3 to map),
            invalidShards = mapOf(n1 to emptyList(), n2 to listOf(5000))
        )

        assertEquals(mapOf(n2 to listOf(5000)), result.invalidShards)
        assertTrue(result.isComplete)
        assertFalse(result.isClean)
    }

    @Test
    fun testUnknownOwners() {
        val gone = NodeId("node-09")
        val newest = map +
            (ShardId.of(17) to stableShard(17, gone)) +
            (ShardId.of(18) to Shard(ShardId.of(18), gone, null, gone, Instant.EPOCH))

        val result = check(merged = newest, copies = mapOf(n1 to newest, n2 to newest, n3 to newest))

        assertEquals(mapOf(gone to shards(17, 18)), result.unknownOwners)
        assertTrue(result.isComplete)
        assertFalse(result.isClean)
    }

    // ---------- helpers ----------

    private fun check(
        merged: Map<ShardId, Shard> = map,
        copies: Map<NodeId, Map<ShardId, Shard>>,
        invalidShards: Map<NodeId, List<Int>> = emptyMap()
    ): ShardMapResult = ShardMapState(setOf(n1, n2, n3), merged, copies, invalidShards).compute()

    private fun shards(vararg shards: Int): List<ShardId> = shards.map { ShardId.of(it) }

    private fun stableShard(shard: Int, node: NodeId, revision: Instant = Instant.EPOCH): Shard =
        Shard(ShardId.of(shard), node, node, null, revision)

    private companion object {
        val NEWER: Instant = Instant.EPOCH.plusSeconds(60)
    }
}
