package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant

class SpreadEvenlyTest {

    private val n1 = NodeId("node-01")
    private val n2 = NodeId("node-02")
    private val n3 = NodeId("node-03")

    @Test
    fun testNewCluster_NodesTakeTurns() {
        val owners = ShardSchema.spreadEvenly(listOf(n1, n2, n3), emptyList(), shards(ShardId.SHARDS_RANGE))

        assertEquals(listOf(n1, n2, n3, n1, n2, n3), (0..5).map { owners.getValue(ShardId.of(it)) })
        assertEquals(mapOf(n1 to 342, n2 to 341, n3 to 341), countPerNode(owners.values))
    }

    @Test
    fun testTieGoesToTheSmallestId() {
        // The order the nodes come in doesn't matter
        val owners = ShardSchema.spreadEvenly(listOf(n3, n1, n2), emptyList(), shards(0..2))

        assertEquals(listOf(n1, n2, n3), owners.values.toList())
    }

    @Test
    fun testMissingShardsEvenTheNodesOut() {
        // node-01 and node-02 own 340 shards each, node-03 334, and 10 shards have no row
        val existing = ownedBy(n1, 0..339) + ownedBy(n2, 340..679) + ownedBy(n3, 680..1013)

        val owners = ShardSchema.spreadEvenly(listOf(n1, n2, n3), existing, shards(1014..1023))

        // The first six go to node-03, then the nodes take turns again
        assertEquals(List(6) { n3 } + listOf(n1, n2, n3, n1), owners.values.toList())
        val total = existing.map { shard -> shard.producerNode } + owners.values
        assertEquals(mapOf(n1 to 342, n2 to 341, n3 to 341), countPerNode(total))
    }

    @Test
    fun testShardsOfUnknownNodesCountForNothing() {
        // node-x is not a node of this cluster, so its 500 shards make no node look busy
        val existing = ownedBy(NodeId("node-x"), 0..499) + ownedBy(n1, 500..509)

        val owners = ShardSchema.spreadEvenly(listOf(n1, n2), existing, shards(510..519))

        // node-02 catches up with node-01
        assertEquals(List(10) { n2 }, owners.values.toList())
    }

    @Test
    fun testMigratingShardCountsForItsTarget() {
        // Shard 0 is moving to node-02, so it counts for node-02, the way the shard balance check counts it
        val existing = listOf(Shard(ShardId.of(0), n2, null, n2, Instant.EPOCH))

        val owners = ShardSchema.spreadEvenly(listOf(n1, n2), existing, shards(1..1))

        assertEquals(n1, owners.getValue(ShardId.of(1)))
    }

    @Test
    fun testOneNodeGetsEverything() {
        val owners = ShardSchema.spreadEvenly(listOf(n1), emptyList(), shards(ShardId.SHARDS_RANGE))

        assertEquals(mapOf(n1 to ShardId.SHARD_COUNT), countPerNode(owners.values))
    }

    @Test
    fun testNoNodes() {
        assertThrows<IllegalArgumentException> {
            ShardSchema.spreadEvenly(emptyList(), emptyList(), shards(ShardId.SHARDS_RANGE))
        }
    }

    private fun shards(numbers: IntRange): List<ShardId> = numbers.map { ShardId.of(it) }

    private fun ownedBy(node: NodeId, numbers: IntRange): List<Shard> =
        numbers.map { number -> Shard(ShardId.of(number), node, node, null, Instant.EPOCH) }

    private fun countPerNode(nodes: Collection<NodeId>): Map<NodeId, Int> = nodes.groupingBy { it }.eachCount()
}
