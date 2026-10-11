package kolbasa.cluster.butcher.check

import kolbasa.butcher.MigrationStateResult
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class MigrationStateResultRenderTest {

    @Test
    fun testClean() {
        assertEquals("Migration state: no shards in migration", MigrationStateResult(emptyMap()).render())
    }

    @Test
    fun testWithMigrations_ContainsShardAndTarget() {
        val n2 = NodeId("n2")
        val n3 = NodeId("n3")
        // Deliberately out of order: the renderer is the one that sorts, both the targets and the shards
        val result = MigrationStateResult(
            mapOf(
                n3 to listOf(50, 40, 30).map(ShardId::of),
                n2 to listOf(10, 20).map(ShardId::of),
            )
        )

        val text = result.render()

        assertTrue(text.contains("5 shard(s) in migration"), text)
        assertTrue(text.contains("⟶ n2"), text)
        assertTrue(text.contains("⟶ n3"), text)
        // Targets sorted by id (n2 before n3) and shards sorted ascending within each group.
        val n2Pos = text.indexOf("⟶ n2")
        val n3Pos = text.indexOf("⟶ n3")
        assertTrue(n2Pos < n3Pos, "n2 group should come before n3 group:\n$text")
        // Check shards list
        assertTrue(text.contains("shards: 10,20"), text)
        assertTrue(text.contains("shards: 30,40,50"), text)
    }

}
