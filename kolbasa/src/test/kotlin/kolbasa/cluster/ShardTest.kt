package kolbasa.cluster

import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ShardTest {

    @Test
    fun testShardBoundaries() {
        assertEquals(0, Shard.MIN_SHARD)
        assertEquals(Shard.SHARD_COUNT, Shard.MAX_SHARD + 1)
    }

    @Test
    fun testInitialConditions() {
        // Wrong shard
        assertThrows<IllegalStateException> {
            Shard(Shard.MAX_SHARD + 1, NodeId("a"), NodeId("a"), null)
        }

        // Wrong stable state
        assertThrows<IllegalStateException> {
            Shard(Shard.MAX_SHARD, NodeId("a"), NodeId("b"), null)
        }

        // Wrong migration state
        assertThrows<IllegalStateException> {
            Shard(Shard.MAX_SHARD, NodeId("a"), null, NodeId("b"))
        }

        // Test good states
        Shard(Shard.MAX_SHARD, NodeId("a"), NodeId("a"), null)
        Shard(Shard.MAX_SHARD, NodeId("a"), null, NodeId("a"))
    }

}
