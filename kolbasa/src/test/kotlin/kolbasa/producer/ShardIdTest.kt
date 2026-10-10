package kolbasa.producer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Test

class ShardIdTest {

    @Test
    fun testShardBoundaries() {
        assertEquals(0, ShardId.MIN_SHARD)
        assertEquals(ShardId.SHARD_COUNT, ShardId.MAX_SHARD + 1)
    }

    @Test
    fun testInitialConditions() {
        // Wrong shard
        assertThrows<IllegalStateException> {
            ShardId.of(ShardId.MAX_SHARD + 1)
        }
    }
}
