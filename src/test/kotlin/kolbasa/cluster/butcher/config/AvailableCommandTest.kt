package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows
import java.io.File

class AvailableCommandTest {

    // -------------------- fromCommandName --------------------

    @Test
    fun testFromCommandName_KnownNames() {
        assertEquals(AvailableCommand.CHECK_CLUSTER, AvailableCommand.fromCommandName("check-cluster"))
        assertEquals(AvailableCommand.PREPARE_MIGRATION, AvailableCommand.fromCommandName("prepare-migration"))
        assertEquals(AvailableCommand.MOVE_DATA, AvailableCommand.fromCommandName("move-data"))
        assertEquals(AvailableCommand.FINALIZE_MIGRATION, AvailableCommand.fromCommandName("finalize-migration"))
    }

    @Test
    fun testFromCommandName_UnknownReturnsNull() {
        assertNull(AvailableCommand.fromCommandName("nope"))
        assertNull(AvailableCommand.fromCommandName(""))
        assertNull(AvailableCommand.fromCommandName("CHECK-CLUSTER"))
    }

    @Test
    fun testCommandNamesAreUnique() {
        val names = AvailableCommand.entries.map { it.commandName }
        assertEquals(names.size, names.toSet().size)
    }

    // -------------------- CHECK_CLUSTER.parse --------------------

    @Test
    fun testCheckCluster_Parse_NoCheckName_RunsAllChecks() {
        val file = configFile()
        val parsed = AvailableCommand.CHECK_CLUSTER.parse(arrayOf("check-cluster", file.absolutePath))

        assertInstanceOf<Command.Check>(parsed)
        assertEquals(AvailableCheck.all, parsed.checks)
    }

    @Test
    fun testCheckCluster_Parse_AllKeyword_RunsAllChecks() {
        val file = configFile()
        val parsed = AvailableCommand.CHECK_CLUSTER.parse(
            arrayOf("check-cluster", "all", file.absolutePath)
        )

        assertInstanceOf<Command.Check>(parsed)
        assertEquals(AvailableCheck.all, parsed.checks)
    }

    @Test
    fun testCheckCluster_Parse_SpecificCheck_RunsOnlyThatCheck() {
        val file = configFile()
        val parsed = AvailableCommand.CHECK_CLUSTER.parse(
            arrayOf("check-cluster", "shard-balance", file.absolutePath)
        )

        assertInstanceOf<Command.Check>(parsed)
        assertEquals(setOf(AvailableCheck.CHECK_SHARD_BALANCE), parsed.checks)
    }

    @Test
    fun testCheckCluster_Parse_UnknownCheck_TreatedAsFilePath() {
        // An unrecognized first arg is treated as a file path; the command falls back to "all checks".
        val file = configFile()
        val parsed = AvailableCommand.CHECK_CLUSTER.parse(
            arrayOf("check-cluster", file.absolutePath)
        )

        assertInstanceOf<Command.Check>(parsed)
        assertEquals(AvailableCheck.all, parsed.checks)
        assertEquals(1, parsed.nodes.dataSources.size)
    }

    @Test
    fun testCheckCluster_Parse_NoArgs_WrapsUsage() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            AvailableCommand.CHECK_CLUSTER.parse(arrayOf("check-cluster"))
        }
        assertTrue(ex.messageToShow.contains("No cluster config files"), ex.messageToShow)
        // wrapWithUsage appends the command's fullUsage
        assertTrue(ex.messageToShow.contains("check-cluster"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("shard-balance"), ex.messageToShow)
    }

    @Test
    fun testCheckCluster_Parse_BuildsClusterNodes() {
        val file = configFile("node-01 host=h1 dbname=db\nnode-02 host=h2 dbname=db\n")
        val parsed = AvailableCommand.CHECK_CLUSTER.parse(arrayOf("check-cluster", file.absolutePath))

        assertEquals(2, parsed.nodes.dataSources.size)
    }

    @Test
    fun testCheckCluster_FullUsage_MentionsEveryCheck() {
        val usage = AvailableCommand.CHECK_CLUSTER.fullUsage
        AvailableCheck.entries.forEach {
            assertTrue(usage.contains(it.checkName), "fullUsage missing ${it.checkName}")
        }
        assertTrue(usage.contains("all"), usage)
    }

    // -------------------- PREPARE_MIGRATION.parse --------------------

    @Test
    fun testPrepareMigration_Parse_AllFieldsSet() {
        val file = configFile()
        val parsed = AvailableCommand.PREPARE_MIGRATION.parse(
            arrayOf("prepare-migration", "--target=node-01", "--shards=0,1,5", file.absolutePath)
        )

        assertInstanceOf<Command.Prepare>(parsed)
        assertEquals(NodeId("node-01"), parsed.target)
        assertEquals(listOf(0, 1, 5), parsed.shards)
    }

    @Test
    fun testPrepareMigration_Parse_ShardsWithWhitespace() {
        val file = configFile()
        val parsed = AvailableCommand.PREPARE_MIGRATION.parse(
            arrayOf("prepare-migration", "--target=node-01", "--shards= 0 , 1 , 2 ", file.absolutePath)
        )

        assertInstanceOf<Command.Prepare>(parsed)
        assertEquals(listOf(0, 1, 2), parsed.shards)
    }

    @Test
    fun testPrepareMigration_Parse_SingleShard() {
        val file = configFile()
        val parsed = AvailableCommand.PREPARE_MIGRATION.parse(
            arrayOf("prepare-migration", "--target=node-01", "--shards=7", file.absolutePath)
        )

        assertInstanceOf<Command.Prepare>(parsed)
        assertEquals(listOf(7), parsed.shards)
    }

    @Test
    fun testPrepareMigration_Parse_MissingTargetThrows() {
        val file = configFile()
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--shards=0", file.absolutePath)
            )
        }
        assertTrue(ex.messageToShow.contains("--target"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("prepare-migration"), ex.messageToShow)
    }

    @Test
    fun testPrepareMigration_Parse_MissingShardsThrows() {
        val file = configFile()
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--target=node-01", file.absolutePath)
            )
        }
        assertTrue(ex.messageToShow.contains("--shards"), ex.messageToShow)
    }

    @Test
    fun testPrepareMigration_Parse_NonNumericShardThrows() {
        val file = configFile()
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--target=node-01", "--shards=0,abc,2", file.absolutePath)
            )
        }
        assertTrue(ex.messageToShow.contains("Invalid shard"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("abc"), ex.messageToShow)
    }

    @Test
    fun testPrepareMigration_Parse_UnknownFlagThrows_AndWrapsUsage() {
        val file = configFile()
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--bogus=x", "--target=n", "--shards=0", file.absolutePath)
            )
        }
        assertTrue(ex.messageToShow.contains("Unknown flag"), ex.messageToShow)
        // wrapped with fullUsage
        assertTrue(ex.messageToShow.contains("prepare-migration"), ex.messageToShow)
    }

    // -------------------- MOVE_DATA.parse --------------------

    @Test
    fun testMoveData_Parse_NoTables_YieldsNull() {
        val file = configFile()
        val parsed = AvailableCommand.MOVE_DATA.parse(arrayOf("move-data", file.absolutePath))

        assertInstanceOf<Command.Move>(parsed)
        assertNull(parsed.tables)
    }

    @Test
    fun testMoveData_Parse_WithTables() {
        val file = configFile()
        val parsed = AvailableCommand.MOVE_DATA.parse(
            arrayOf("move-data", "--tables=orders,events,logs", file.absolutePath)
        )

        assertInstanceOf<Command.Move>(parsed)
        assertEquals(setOf("orders", "events", "logs"), parsed.tables)
    }

    @Test
    fun testMoveData_Parse_TablesWhitespaceTrimmed() {
        val file = configFile()
        val parsed = AvailableCommand.MOVE_DATA.parse(
            arrayOf("move-data", "--tables= orders , events ", file.absolutePath)
        )

        assertInstanceOf<Command.Move>(parsed)
        assertEquals(setOf("orders", "events"), parsed.tables)
    }

    @Test
    fun testMoveData_Parse_TablesDuplicatesCollapse() {
        val file = configFile()
        val parsed = AvailableCommand.MOVE_DATA.parse(
            arrayOf("move-data", "--tables=orders,orders,events", file.absolutePath)
        )

        assertInstanceOf<Command.Move>(parsed)
        assertEquals(setOf("orders", "events"), parsed.tables)
    }

    @Test
    fun testMoveData_Parse_NoFilesThrows() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            AvailableCommand.MOVE_DATA.parse(arrayOf("move-data"))
        }
        assertTrue(ex.messageToShow.contains("No cluster config files"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("move-data"), ex.messageToShow)
    }

    // -------------------- FINALIZE_MIGRATION.parse --------------------

    @Test
    fun testFinalizeMigration_Parse_Ok() {
        val file = configFile()
        val parsed = AvailableCommand.FINALIZE_MIGRATION.parse(
            arrayOf("finalize-migration", file.absolutePath)
        )

        assertInstanceOf<Command.Finalize>(parsed)
        assertEquals(AvailableCommand.FINALIZE_MIGRATION, parsed.command)
    }

    @Test
    fun testFinalizeMigration_Parse_NoFilesThrows() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            AvailableCommand.FINALIZE_MIGRATION.parse(arrayOf("finalize-migration"))
        }
        assertTrue(ex.messageToShow.contains("No cluster config files"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("finalize-migration"), ex.messageToShow)
    }

    @Test
    fun testFinalizeMigration_Parse_RejectsUnknownFlag() {
        val file = configFile()
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            AvailableCommand.FINALIZE_MIGRATION.parse(
                arrayOf("finalize-migration", "--target=x", file.absolutePath)
            )
        }
        assertTrue(ex.messageToShow.contains("Unknown flag"), ex.messageToShow)
    }

    // -------------------- Per-command meta --------------------

    private fun configFile(content: String = "node-01 host=h1 dbname=db\n"): File {
        val file = File.createTempFile("kolbasa-available-command-", null)
        file.writeText(content)
        file.deleteOnExit()
        return file
    }
}
