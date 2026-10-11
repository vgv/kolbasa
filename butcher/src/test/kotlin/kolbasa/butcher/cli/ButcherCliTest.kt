package kolbasa.butcher.cli

import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.testing.test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

/**
 * The command line as an operator meets it
 *
 * Every case here is rejected before the cluster is touched, so none of them needs a database: Clikt
 * finishes parsing and validating before a command's body runs.
 */
class ButcherCliTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun testHelpListsEveryCommand() {
        val result = cli().test("--help")

        assertEquals(0, result.statusCode, result.output)
        listOf("check-cluster", "prepare-migration", "move-data", "finalize-migration").forEach {
            assertTrue(result.output.contains(it), "'$it' missing from:\n${result.output}")
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
        PrepareMigration(),
        MoveData(),
        FinalizeMigration(),
    )

    private fun config(): File = File(tempDir, "cluster.conf").apply {
        writeText("node-01  host=localhost port=5432 dbname=orders\n")
    }

}
