package kolbasa.cluster

import kolbasa.producer.ShardId
import kolbasa.schema.Const
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ShardsTest {

    @Test
    fun testAsWhereClause() {
        val shards = Shards(listOf(ShardId.of(1), ShardId.of(2), ShardId.of(3)))
        assertEquals("${Const.SHARD_COLUMN_NAME} in (1,2,3)", shards.asWhereClause)
    }

    @Test
    fun testAllShards() {
        assertEquals(ShardId.MIN_SHARD_ID, Shards.ALL_SHARDS.shards.minOrNull())
        assertEquals(ShardId.MAX_SHARD_ID, Shards.ALL_SHARDS.shards.maxOrNull())
        assertEquals(ShardId.SHARD_COUNT, Shards.ALL_SHARDS.shards.size)
    }

}
