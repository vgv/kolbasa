package kolbasa.cluster

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ShardsTest {

    @Test
    fun testAsText() {
        val shards = Shards(listOf(1, 2, 3))
        assertEquals("1,2,3", shards.asText)
    }

    @Test
    fun testAllShards() {
        assertEquals(Shard.MIN_SHARD, Shards.ALL_SHARDS.shards.minOrNull())
        assertEquals(Shard.MAX_SHARD, Shards.ALL_SHARDS.shards.maxOrNull())
        assertEquals(Shard.SHARD_COUNT, Shards.ALL_SHARDS.shards.size)
    }

}
