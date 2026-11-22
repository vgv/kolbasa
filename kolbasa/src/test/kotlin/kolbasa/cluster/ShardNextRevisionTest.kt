package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class ShardNextRevisionTest {

    private val now = Instant.parse("2026-11-24T10:00:00.123456789Z")

    // PostgreSQL keeps microseconds
    private val nowInMicros = Instant.parse("2026-11-24T10:00:00.123456Z")

    @Test
    fun testNextRevision_OlderShards_CurrentTime() {
        assertEquals(nowInMicros, ShardSchema.nextRevision(emptyList(), now))
        assertEquals(nowInMicros, ShardSchema.nextRevision(listOf(shard(1, Instant.EPOCH)), now))
    }

    @Test
    fun testNextRevision_ClockIsBehind_NewerThanEveryShard() {
        val newest = Instant.parse("2026-11-24T10:05:00.000007Z")
        val shards = listOf(shard(1, Instant.EPOCH), shard(2, newest))

        assertEquals(Instant.parse("2026-11-24T10:05:00.000008Z"), ShardSchema.nextRevision(shards, now))
    }

    @Test
    fun testNextRevision_SameMicrosecond_NewerThanEveryShard() {
        assertEquals(nowInMicros.plusNanos(1000), ShardSchema.nextRevision(listOf(shard(1, nowInMicros)), now))
    }

    private fun shard(shard: Int, revision: Instant): Shard {
        return Shard(ShardId.of(shard), NodeId("node-01"), NodeId("node-01"), null, revision)
    }
}
