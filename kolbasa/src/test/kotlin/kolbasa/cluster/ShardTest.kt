package kolbasa.cluster

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ShardTest {

    @Test
    fun testInitialConditions() {
        // Wrong stable state
        assertThrows<IllegalStateException> {
            Shard(ShardId.of(ShardId.MAX_SHARD), NodeId("a"), NodeId("b"), null)
        }

        // Wrong migration state
        assertThrows<IllegalStateException> {
            Shard(ShardId.of(ShardId.MAX_SHARD), NodeId("a"), null, NodeId("b"))
        }

        // Test good states
        Shard(ShardId.of(ShardId.MAX_SHARD), NodeId("a"), NodeId("a"), null)
        Shard(ShardId.of(ShardId.MAX_SHARD), NodeId("a"), null, NodeId("a"))
    }

}
