package kolbasa.butcher

import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.testing.test
import kolbasa.butcher.command.CheckCluster
import kolbasa.butcher.command.CreateOrRepairShardMap
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
            "check-cluster", "rebalance-cluster", "drain-nodes", "prepare-migration", "move-data", "finalize-migration",
            "create-or-repair-shard-map"
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
            "create-or-repair-shard-map" to "Low-level commands:",
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
        val result = cli().test("-c ${config().absolutePath} prepare-migration --shards=0")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--target"), result.output)
    }

    @Test
    fun testMissingShardsIsReported() {
        val result = cli().test("-c ${config().absolutePath} prepare-migration --target=node-01")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--shards"), result.output)
    }

    @Test
    fun testShardOutOfRangeIsReported() {
        val result = cli().test("-c ${config().absolutePath} prepare-migration --target=node-01 --shards=0,9999")

        assertEquals(1, result.statusCode, result.output)
        // ShardId.of() would have folded 9999 into a valid shard, so the range is checked where the
        // operator's input arrives instead
        assertTrue(result.output.contains("9999"), result.output)
        assertTrue(result.output.contains("out of range"), result.output)
    }

    @Test
    fun testNonNumericShardIsReported() {
        val result = cli().test("-c ${config().absolutePath} prepare-migration --target=node-01 --shards=zero")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--shards"), result.output)
    }

    @Test
    fun testUnknownCheckNameIsReported() {
        val result = cli().test("-c ${config().absolutePath} check-cluster --checks=whatever")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("--checks"), result.output)
    }

    @Test
    fun testMutatingCommandRefusesToGuessWithoutATerminal() {
        // No --yes and nothing to read an answer from: a tool that moves production data must not take
        // silence for consent, and must not hang waiting for it either
        val result = cli().test("-c ${config().absolutePath} finalize-migration")

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
    fun testCheckClusterHelpNamesItsExitCodes() {
        val result = cli().test("check-cluster --help")

        assertEquals(0, result.statusCode, result.output)
        val text = result.output.replace(Regex("\\s+"), " ")
        assertTrue(text.contains("Exits 0 when every check passed, 2 when a check found something"), result.output)
    }

    @Test
    fun testMutatingCommandsOfferYes() {
        listOf(
            "rebalance-cluster", "drain-nodes", "prepare-migration", "move-data", "finalize-migration",
            "create-or-repair-shard-map"
        ).forEach { command ->
            val result = cli().test("$command --help")

            assertEquals(0, result.statusCode, result.output)
            assertTrue(result.output.contains("--yes"), "$command:\n${result.output}")
        }
    }

    @Test
    fun testShardsPerRoundOutOfRangeIsReported() {
        listOf("0", "-5", "2000").forEach { value ->
            val result = cli().test("-c ${config().absolutePath} rebalance-cluster --shards-per-round=$value")

            assertEquals(1, result.statusCode, result.output)
            assertTrue(result.output.contains("shards per round"), "$value:\n${result.output}")
        }
    }

    @Test
    fun testMissingNodesToDrainAreReported() {
        val result = cli().test("-c ${config().absolutePath} drain-nodes --shards-per-round=10")

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

        val result = cli().test("-c ${deadNode.absolutePath} check-cluster")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("Can't connect to 1 of 1"), result.output)
        assertTrue(result.output.contains("refused"), result.output)
        assertTrue(!result.output.contains("request timed out"), result.output)
    }

    @Test
    fun testSeveralConfigFilesAreMerged() {
        // Every -c is read: the nodes of both files are asked whether they are there
        val first = File(tempDir, "first.conf").apply {
            writeText("line-1  host=127.0.0.1 port=9 dbname=orders\n")
        }
        val second = File(tempDir, "second.conf").apply {
            writeText("line-2  host=127.0.0.1 port=9 dbname=orders\n")
        }

        val result = cli().test("-c ${first.absolutePath} --config ${second.absolutePath} check-cluster")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("Can't connect to 2 of 2"), result.output)
    }

    @Test
    fun testMissingConfigFileIsReported() {
        val result = cli().test("-c /no/such/cluster.conf finalize-migration")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("does not exist"), result.output)
    }

    @Test
    fun testConfigFileIsRequired() {
        // finalize-migration asks before it does anything. Without a config it must not get that far
        val result = cli().test("finalize-migration")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("missing option --config"), result.output)
        assertTrue(!result.output.contains("no terminal"), result.output)
    }

    @Test
    fun testBadConfigFileIsReportedBeforeTheQuestion() {
        val noHost = File(tempDir, "no-host.conf").apply {
            writeText("node-01  dbname=orders\n")
        }

        val result = cli().test("-c ${noHost.absolutePath} finalize-migration")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("host"), result.output)
        assertTrue(!result.output.contains("no terminal"), result.output)
    }

    @Test
    fun testHelpShowsTheConfig() {
        val root = cli().test("--help")
        assertTrue(root.output.contains("-c, --config=<file>"), root.output)

        // Clikt doesn't show the options of butcher itself in a command's help, so the command says where -c goes
        val command = cli().test("check-cluster --help")
        // The help is wrapped to the terminal width, and a line may break inside the example
        val text = command.output.replace(Regex("\\s+"), " ")
        assertTrue(text.contains("-c cluster.conf check-cluster"), command.output)
    }

    private fun cli() = ButcherCli().subcommands(
        CheckCluster(),
        RebalanceCluster(),
        DrainNodes(),
        PrepareMigration(),
        MoveData(),
        FinalizeMigration(),
        CreateOrRepairShardMap(),
    )

    private fun config(): File = File(tempDir, "cluster.conf").apply {
        writeText("node-01  host=localhost port=5432 dbname=orders\n")
    }

}
