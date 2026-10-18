package kolbasa.butcher

import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.testing.test
import kolbasa.butcher.command.CheckCluster
import kolbasa.butcher.command.DrainNodes
import kolbasa.butcher.command.FinalizeMigration
import kolbasa.butcher.command.MoveData
import kolbasa.butcher.command.PrepareMigration
import kolbasa.butcher.command.RebalanceCluster
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

/**
 * The command line as an operator meets it
 *
 * Almost every case here is rejected before the cluster is touched, so it needs no database: Clikt
 * finishes parsing and validating before a command's body runs. The exception is the unreachable node,
 * which needs nothing but a closed port.
 */
class ButcherCliTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun testHelpListsEveryCommand() {
        val result = cli().test("--help")

        assertEquals(0, result.statusCode, result.output)
        listOf(
            "check-cluster", "rebalance-cluster", "drain-nodes", "prepare-migration", "move-data", "finalize-migration"
        ).forEach {
            assertTrue(result.output.contains(it), "'$it' missing from:\n${result.output}")
        }
    }

    @Test
    fun testHelpGroupsTheCommandsByWhatTheyAre() {
        val result = cli().test("--help")

        assertEquals(0, result.statusCode, result.output)
        // Two levels of command, which one flat list would hide
        val sections = listOf("Diagnostics:", "High-level commands:", "Low-level commands:")
        sections.forEach { assertTrue(result.output.contains(it), "'$it' missing from:\n${result.output}") }
        // In this order, which is the order they are reached for in
        assertEquals(
            sections,
            sections.sortedBy { result.output.indexOf(it) },
            result.output
        )
        // The section is a layout instruction, so it must not leak into the text as a tag
        assertTrue(!result.output.contains("butcher-help-section"), result.output)
    }

    @Test
    fun testEveryCommandIsListedUnderItsOwnSection() {
        val output = cli().test("--help").output

        mapOf(
            "check-cluster" to "Diagnostics:",
            "rebalance-cluster" to "High-level commands:",
            "drain-nodes" to "High-level commands:",
            "prepare-migration" to "Low-level commands:",
            "move-data" to "Low-level commands:",
            "finalize-migration" to "Low-level commands:",
        ).forEach { (command, section) ->
            // The section a command is listed under is the last one printed before it
            val sectionsBefore = listOf("Diagnostics:", "High-level commands:", "Low-level commands:")
                .filter { output.indexOf(it) in 0 until output.indexOf("  $command")  }
            assertEquals(section, sectionsBefore.lastOrNull(), "$command:\n$output")
        }
    }

    @Test
    fun testNoArgumentsShowsUsage() {
        val result = cli().test("")

        // Clikt's convention, and a change from the hand-rolled parser that exited 1 here: a bare
        // invocation prints the help to stderr and exits 0, it is a question rather than a failure
        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("Usage: java -jar butcher.jar"), result.output)
        assertTrue(result.output.contains("check-cluster"), result.output)
    }

    @Test
    fun testUnknownCommandFails() {
        val result = cli().test("do-something-clever")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("no such subcommand"), result.output)
    }

    @Test
    fun testMissingTargetIsReported() {
        val result = cli().test("prepare-migration --shards=0 ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--target"), result.output)
    }

    @Test
    fun testMissingShardsIsReported() {
        val result = cli().test("prepare-migration --target=node-01 ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--shards"), result.output)
    }

    @Test
    fun testShardOutOfRangeIsReported() {
        val result = cli().test("prepare-migration --target=node-01 --shards=0,9999 ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        // ShardId.of() would have folded 9999 into a valid shard, so the range is checked where the
        // operator's input arrives instead
        assertTrue(result.output.contains("9999"), result.output)
        assertTrue(result.output.contains("out of range"), result.output)
    }

    @Test
    fun testNonNumericShardIsReported() {
        val result = cli().test("prepare-migration --target=node-01 --shards=zero ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--shards"), result.output)
    }

    @Test
    fun testUnknownCheckNameIsReported() {
        val result = cli().test("check-cluster --checks=whatever ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--checks"), result.output)
    }

    @Test
    fun testMutatingCommandRefusesToGuessWithoutATerminal() {
        // No --yes and nothing to read an answer from: a tool that moves production data must not take
        // silence for consent, and must not hang waiting for it either
        val result = cli().test("finalize-migration ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("no terminal"), result.output)
        assertTrue(result.output.contains("--yes"), result.output)
    }

    @Test
    fun testCheckClusterNeedsNoConfirmation() {
        // It changes nothing, so it must not have a --yes to forget about
        val result = cli().test("check-cluster --help")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(!result.output.contains("--yes"), result.output)
    }

    @Test
    fun testMutatingCommandsOfferYes() {
        listOf("rebalance-cluster", "drain-nodes", "prepare-migration", "move-data", "finalize-migration").forEach { command ->
            val result = cli().test("$command --help")

            assertEquals(0, result.statusCode, result.output)
            assertTrue(result.output.contains("--yes"), "$command:\n${result.output}")
        }
    }

    @Test
    fun testShardsPerRoundOutOfRangeIsReported() {
        listOf("0", "-5", "2000").forEach { value ->
            val result = cli().test("rebalance-cluster --shards-per-round=$value ${config().absolutePath}")

            assertEquals(1, result.statusCode, result.output)
            assertTrue(result.output.contains("shards per round"), "$value:\n${result.output}")
        }
    }

    @Test
    fun testMissingNodesToDrainAreReported() {
        val result = cli().test("drain-nodes --shards-per-round=10 ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--nodes"), result.output)
    }

    @Test
    fun testUnreachableNodeIsReportedByTheDriver() {
        // Reachability is checked on a plain driver connection, before any pool is built. A pool would
        // wait out its connection timeout and then report that timeout instead of this
        val deadNode = File(tempDir, "dead.conf").apply {
            writeText("line-1  host=127.0.0.1 port=9 dbname=orders\n")
        }

        val result = cli().test("check-cluster ${deadNode.absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("Can't connect to 1 of 1"), result.output)
        assertTrue(result.output.contains("refused"), result.output)
        assertTrue(!result.output.contains("request timed out"), result.output)
    }

    @Test
    fun testMissingConfigFileIsReported() {
        val result = cli().test("finalize-migration /no/such/cluster.conf")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("does not exist"), result.output)
    }

    @Test
    fun testConfigFileIsRequired() {
        val result = cli().test("finalize-migration")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("config-file"), result.output)
    }

    private fun cli() = ButcherCli().subcommands(
        CheckCluster(),
        RebalanceCluster(),
        DrainNodes(),
        PrepareMigration(),
        MoveData(),
        FinalizeMigration(),
    )

    private fun config(): File = File(tempDir, "cluster.conf").apply {
        writeText("node-01  host=localhost port=5432 dbname=orders\n")
    }

}
