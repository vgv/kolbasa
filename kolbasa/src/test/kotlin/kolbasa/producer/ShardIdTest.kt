package kolbasa.producer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ShardIdTest {

    @Test
    fun testShardBoundaries() {
        assertEquals(0, ShardId.MIN_SHARD)
        assertEquals(ShardId.SHARD_COUNT, ShardId.MAX_SHARD + 1)
    }

    @Test
    fun testEqualsAndHashCode() {
        val first = ShardId.of(42)
        val second = ShardId.of(42)
        val third = ShardId.of(43)

        assertSame(first, second)
        assertEquals(first, second)
        assertEquals(first.hashCode(), second.hashCode())

        assertNotEquals(first, third)
    }

    @Test
    fun testEveryShardIsCached() {
        ShardId.SHARDS_RANGE.forEach { shard ->
            assertSame(ShardId.of(shard), ShardId.of(shard), "shard=$shard")
            assertEquals(shard, ShardId.of(shard).id)
        }
    }

    @Test
    fun testComparison() {
        val min = ShardId.of(ShardId.MIN_SHARD)
        val middle = ShardId.of(ShardId.MAX_SHARD / 2)
        val max = ShardId.of(ShardId.MAX_SHARD)

        assertEquals(0, min.compareTo(min))
        assertTrue(min < middle)
        assertTrue(middle < max)
        assertTrue(min < max)
    }

    @Test
    fun testToString() {
        assertEquals("shard[5]", ShardId.of(5).toString())
    }

    @Test
    fun testInitialConditions() {
        // Wrong shard
        assertThrows<IllegalStateException> {
            ShardId.of(ShardId.MAX_SHARD + 1)
        }
        assertThrows<IllegalStateException> {
            ShardId.of(ShardId.MIN_SHARD - 1)
        }

        // Boundaries themselves are valid
        assertEquals(ShardId.MIN_SHARD, ShardId.of(ShardId.MIN_SHARD).id)
        assertEquals(ShardId.MAX_SHARD, ShardId.of(ShardId.MAX_SHARD).id)
    }
}
