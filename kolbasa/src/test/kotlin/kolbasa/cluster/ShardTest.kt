package kolbasa.cluster

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant

class ShardTest {

    @Test
    fun testInitialConditions() {
        // Wrong stable state
        assertThrows<IllegalStateException> {
            Shard(ShardId.of(ShardId.MAX_SHARD), NodeId("a"), NodeId("b"), null, Instant.EPOCH)
        }

        // Wrong migration state
        assertThrows<IllegalStateException> {
            Shard(ShardId.of(ShardId.MAX_SHARD), NodeId("a"), null, NodeId("b"), Instant.EPOCH)
        }

        // Test good states
        Shard(ShardId.of(ShardId.MAX_SHARD), NodeId("a"), NodeId("a"), null, Instant.EPOCH)
        Shard(ShardId.of(ShardId.MAX_SHARD), NodeId("a"), null, NodeId("a"), Instant.EPOCH)
    }

    @Test
    fun testEqualsIgnoreRevision() {
        val shard = Shard(ShardId.of(5), NodeId("node-01"), NodeId("node-01"), null, Instant.EPOCH)

        // Only the revision differs
        assertTrue(shard.equalsIgnoreRevision(Shard(ShardId.of(5), NodeId("node-01"), NodeId("node-01"), null, Instant.now())))

        // Another owner
        assertFalse(shard.equalsIgnoreRevision(Shard(ShardId.of(5), NodeId("node-02"), NodeId("node-02"), null, Instant.EPOCH)))
    }

    @Test
    fun testEquals() {
        val shard = Shard(ShardId.of(5), NodeId("node-01"), NodeId("node-01"), null, Instant.EPOCH)
        val sameShard = Shard(ShardId.of(5), NodeId("node-01"), NodeId("node-01"), null, Instant.EPOCH)

        assertEquals(shard, sameShard)
        assertEquals(shard.hashCode(), sameShard.hashCode())

        // Every field takes part, the revision too
        assertNotEquals(shard, Shard(ShardId.of(6), NodeId("node-01"), NodeId("node-01"), null, Instant.EPOCH))
        assertNotEquals(shard, Shard(ShardId.of(5), NodeId("node-02"), NodeId("node-02"), null, Instant.EPOCH))
        assertNotEquals(shard, Shard(ShardId.of(5), NodeId("node-01"), null, NodeId("node-01"), Instant.EPOCH))
        assertNotEquals(shard, Shard(ShardId.of(5), NodeId("node-01"), NodeId("node-01"), null, Instant.EPOCH.plusNanos(1000)))
    }

    @Test
    fun testToString() {
        val stable = Shard(ShardId.of(5), NodeId("node-01"), NodeId("node-01"), null, Instant.EPOCH)
        assertEquals("shard[5,node-01,node-01,null,1970-01-01T00:00:00Z]", stable.toString())

        val migrating = Shard(ShardId.of(5), NodeId("node-02"), null, NodeId("node-02"), Instant.EPOCH)
        assertEquals("shard[5,node-02,null,node-02,1970-01-01T00:00:00Z]", migrating.toString())
    }

}
