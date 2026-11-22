package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class ShardMergeTest {

    private val node1 = Node(NodeId("node-01"), 1)
    private val node2 = Node(NodeId("node-02"), 2)
    private val node3 = Node(NodeId("node-03"), 3)

    @Test
    fun testMergeShards_NewerRevisionWins() {
        val older = copyOf(shard(5, "node-03", Instant.EPOCH))
        val newer = copyOf(shard(5, "node-01", Instant.EPOCH.plusSeconds(60)))

        assertEquals(newer, ShardSchema.mergeShards(sortedMapOf(node1 to older, node2 to newer)))
        assertEquals(newer, ShardSchema.mergeShards(sortedMapOf(node1 to newer, node2 to older)))
    }

    @Test
    fun testMergeShards_SameRevision_RowOfTheSmallestNodeWins() {
        val older = copyOf(shard(5, "node-01", Instant.EPOCH))
        val one = copyOf(shard(5, "node-03", Instant.EPOCH.plusSeconds(60)))
        val another = copyOf(shard(5, "node-02", Instant.EPOCH.plusSeconds(60)))

        // The node that holds the row decides, not the node the row names
        assertEquals(one, ShardSchema.mergeShards(sortedMapOf(node1 to one, node2 to another)))
        assertEquals(another, ShardSchema.mergeShards(sortedMapOf(node1 to another, node2 to one)))

        // Only among the copies with the newest revision
        assertEquals(one, ShardSchema.mergeShards(sortedMapOf(node1 to older, node2 to one, node3 to another)))
    }

    @Test
    fun testMergeShards_EveryShardOfEveryCopy() {
        val first = copyOf(shard(1, "node-01", Instant.EPOCH), shard(2, "node-02", Instant.EPOCH))
        val second = copyOf(shard(3, "node-03", Instant.EPOCH))

        assertEquals(first + second, ShardSchema.mergeShards(sortedMapOf(node1 to first, node2 to emptyMap(), node3 to second)))
    }

    private fun shard(shard: Int, node: String, revision: Instant): Shard {
        return Shard(ShardId.of(shard), NodeId(node), NodeId(node), null, revision)
    }

    private fun copyOf(vararg shards: Shard): Map<ShardId, Shard> {
        return shards.associateBy { it.shard }
    }
}
