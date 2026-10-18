package kolbasa.butcher

import com.github.ajalt.mordant.rendering.AnsiLevel
import com.github.ajalt.mordant.terminal.Terminal
import com.github.ajalt.mordant.terminal.TerminalRecorder
import io.mockk.mockk
import kolbasa.cluster.admin.ShardDiff
import kolbasa.cluster.admin.ShardOwnership
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import javax.sql.DataSource

class RebalanceProgressCallbackTest {

    private val recorder = TerminalRecorder(ansiLevel = AnsiLevel.NONE, width = 120)
    private val terminal = Terminal(terminalInterface = recorder)
    private val callback = RebalanceProgressCallback(terminal)

    @Test
    fun testRoundHeaderNamesEveryTargetAndWhatIsLeft() {
        callback.rebalanceRoundStart(
            round = 3,
            moves = mapOf(NodeId("node-03") to shards(0, 1, 2), NodeId("node-02") to shards(3, 4)),
            shardsLeftToMove = 1_282
        )

        // Nodes in a fixed order, so two runs of the same rebalance read the same
        assertTrue(
            recorder.stdout().contains("Round 3: 2 to node-02, 3 to node-03 (1,282 shard(s) left to move)"),
            recorder.stdout()
        )
    }

    @Test
    fun testStepsPrintCountsAndNotShards() {
        callback.rebalanceRoundStart(1, mapOf(NodeId("node-02") to shards(0, 1)), shardsLeftToMove = 2)
        callback.prepareSuccessful(shards(0, 1), NodeId("node-02"), diffs(0, 1))
        callback.finalizeSuccessful(diffs(0, 1))
        callback.rebalanceFinished(rounds = 1, shardsMoved = 2)

        val output = recorder.stdout()
        assertTrue(output.contains("Prepared 2 shard(s) for node-02"), output)
        assertTrue(output.contains("Finalized 2 shard(s)"), output)
        assertTrue(output.contains("Moved 2 shard(s) in 1 round(s)"), output)
        // The line per shard belongs to a step run by hand
        assertTrue(!output.contains("=>"), output)
    }

    @Test
    fun testCopyingRowsPrintsTheSameAsByHand() {
        val from = mockk<DataSource>(relaxed = true)
        val to = mockk<DataSource>(relaxed = true)

        callback.moveNextBatch("q_orders", from, to, 1_000, 10_000L, 10_200_200L)

        assertTrue(
            recorder.stdout().contains("q_orders: moved 1,000 rows, table: 10,000 rows, total: 10,200,200 rows"),
            recorder.stdout()
        )
    }

    private fun shards(vararg numbers: Int): List<ShardId> = numbers.map(ShardId::of)

    /** One `stable on node-01 => migrating to node-02` diff per shard */
    private fun diffs(vararg numbers: Int): List<ShardDiff> = numbers.map { shard ->
        ShardDiff(
            originalShard = ShardOwnership.Stable(ShardId.of(shard), NodeId("node-01")),
            updatedShard = ShardOwnership.Migrating(ShardId.of(shard), NodeId("node-02"))
        )
    }
}
