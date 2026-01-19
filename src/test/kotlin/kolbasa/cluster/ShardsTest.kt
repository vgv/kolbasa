package kolbasa.cluster

import kolbasa.schema.Const
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ShardsTest {

    @Test
    fun testAsWhereClause() {
        val shards = Shards(listOf(1, 2, 3))
        assertEquals("${Const.SHARD_COLUMN_NAME} in (1,2,3)", shards.asWhereClause)
    }

    @Test
    fun testAllShards() {
        assertEquals(Shard.MIN_SHARD, Shards.ALL_SHARDS.shards.minOrNull())
        assertEquals(Shard.MAX_SHARD, Shards.ALL_SHARDS.shards.maxOrNull())
        assertEquals(Shard.SHARD_COUNT, Shards.ALL_SHARDS.shards.size)
    }

}
