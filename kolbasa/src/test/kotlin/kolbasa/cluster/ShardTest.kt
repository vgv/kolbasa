package kolbasa.cluster

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertFalse
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
        assertTrue(shard.equalsIgnoreRevision(shard.copy(revision = Instant.now())))

        // Another owner
        assertFalse(shard.equalsIgnoreRevision(shard.copy(producerNode = NodeId("node-02"), consumerNode = NodeId("node-02"))))
    }

}
