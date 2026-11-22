package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class ShardMergeTest {

    @Test
    fun testMergeShards_NewerRevisionWins() {
        val older = shard(5, "node-03", Instant.EPOCH)
        val newer = shard(5, "node-01", Instant.EPOCH.plusSeconds(60))

        assertEquals(copyOf(newer), ShardSchema.mergeShards(listOf(copyOf(older), copyOf(newer))))
        assertEquals(copyOf(newer), ShardSchema.mergeShards(listOf(copyOf(newer), copyOf(older))))
    }

    @Test
    fun testMergeShards_SameRevision_SameRowWhateverTheOrder() {
        val one = shard(5, "node-01", Instant.EPOCH)
        val another = shard(5, "node-03", Instant.EPOCH)

        // The greater row, see Shard.compareTo
        assertEquals(copyOf(another), ShardSchema.mergeShards(listOf(copyOf(one), copyOf(another))))
        assertEquals(copyOf(another), ShardSchema.mergeShards(listOf(copyOf(another), copyOf(one))))
    }

    @Test
    fun testMergeShards_EveryShardOfEveryCopy() {
        val first = copyOf(shard(1, "node-01", Instant.EPOCH), shard(2, "node-02", Instant.EPOCH))
        val second = copyOf(shard(3, "node-03", Instant.EPOCH))

        assertEquals(first + second, ShardSchema.mergeShards(listOf(first, emptyMap(), second)))
    }

    private fun shard(shard: Int, node: String, revision: Instant): Shard {
        return Shard(ShardId.of(shard), NodeId(node), NodeId(node), null, revision)
    }

    private fun copyOf(vararg shards: Shard): Map<ShardId, Shard> {
        return shards.associateBy { it.shard }
    }
}
