package kolbasa.butcher.cli.config

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
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.CHECK_CLUSTER.parse(arrayOf("check-cluster"))
        }
        assertTrue(ex.message.contains("No cluster config files"), ex.message)
        // wrapWithUsage appends the command's fullUsage
        assertTrue(ex.message.contains("check-cluster"), ex.message)
        assertTrue(ex.message.contains("shard-balance"), ex.message)
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
        assertEquals(listOf(0, 1, 5), parsed.shards.map { it.id })
    }

    @Test
    fun testPrepareMigration_Parse_ShardsWithWhitespace() {
        val file = configFile()
        val parsed = AvailableCommand.PREPARE_MIGRATION.parse(
            arrayOf("prepare-migration", "--target=node-01", "--shards= 0 , 1 , 2 ", file.absolutePath)
        )

        assertInstanceOf<Command.Prepare>(parsed)
        assertEquals(listOf(0, 1, 2), parsed.shards.map { it.id })
    }

    @Test
    fun testPrepareMigration_Parse_SingleShard() {
        val file = configFile()
        val parsed = AvailableCommand.PREPARE_MIGRATION.parse(
            arrayOf("prepare-migration", "--target=node-01", "--shards=7", file.absolutePath)
        )

        assertInstanceOf<Command.Prepare>(parsed)
        assertEquals(listOf(7), parsed.shards.map { it.id })
    }

    @Test
    fun testPrepareMigration_Parse_ShardOutOfRangeThrows() {
        val file = configFile()
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--target=node-01", "--shards=0,9999", file.absolutePath)
            )
        }

        // ShardId.of() would have folded 9999 into a valid shard, so the range is checked where the
        // operator's input arrives instead
        assertTrue(ex.message.contains("9999"), ex.message)
        assertTrue(ex.message.contains("out of range"), ex.message)
    }

    @Test
    fun testPrepareMigration_Parse_NegativeShardThrows() {
        val file = configFile()
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--target=node-01", "--shards=-1", file.absolutePath)
            )
        }
        assertTrue(ex.message.contains("out of range"), ex.message)
    }

    @Test
    fun testPrepareMigration_Parse_MissingTargetThrows() {
        val file = configFile()
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--shards=0", file.absolutePath)
            )
        }
        assertTrue(ex.message.contains("--target"), ex.message)
        assertTrue(ex.message.contains("prepare-migration"), ex.message)
    }

    @Test
    fun testPrepareMigration_Parse_MissingShardsThrows() {
        val file = configFile()
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--target=node-01", file.absolutePath)
            )
        }
        assertTrue(ex.message.contains("--shards"), ex.message)
    }

    @Test
    fun testPrepareMigration_Parse_NonNumericShardThrows() {
        val file = configFile()
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--target=node-01", "--shards=0,abc,2", file.absolutePath)
            )
        }
        assertTrue(ex.message.contains("Invalid shard"), ex.message)
        assertTrue(ex.message.contains("abc"), ex.message)
    }

    @Test
    fun testPrepareMigration_Parse_UnknownFlagThrows_AndWrapsUsage() {
        val file = configFile()
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.PREPARE_MIGRATION.parse(
                arrayOf("prepare-migration", "--bogus=x", "--target=n", "--shards=0", file.absolutePath)
            )
        }
        assertTrue(ex.message.contains("Unknown flag"), ex.message)
        // wrapped with fullUsage
        assertTrue(ex.message.contains("prepare-migration"), ex.message)
    }

    // -------------------- MOVE_DATA.parse --------------------

    @Test
    fun testMoveData_Parse_NoTables_YieldsNull() {
        val file = configFile()
        val parsed = AvailableCommand.MOVE_DATA.parse(arrayOf("move-data", file.absolutePath))

        assertInstanceOf<Command.Move>(parsed)
        assertNull(parsed.includeTables)
        assertNull(parsed.excludeTables)
    }

    @Test
    fun testMoveData_Parse_WithTables() {
        val file = configFile()
        val parsed = AvailableCommand.MOVE_DATA.parse(
            arrayOf("move-data", "--include-tables=orders,events,logs", "--exclude-tables=garbage,trash", file.absolutePath)
        )

        assertInstanceOf<Command.Move>(parsed)
        assertEquals(setOf("orders", "events", "logs"), parsed.includeTables)
        assertEquals(setOf("garbage", "trash"), parsed.excludeTables)
    }

    @Test
    fun testMoveData_Parse_TablesWhitespaceTrimmed() {
        val file = configFile()
        val parsed = AvailableCommand.MOVE_DATA.parse(
            arrayOf("move-data", "--exclude-tables= garbage , trash ", "--include-tables= orders , events ", file.absolutePath)
        )

        assertInstanceOf<Command.Move>(parsed)
        assertEquals(setOf("orders", "events"), parsed.includeTables)
        assertEquals(setOf("garbage", "trash"), parsed.excludeTables)
    }

    @Test
    fun testMoveData_Parse_TablesDuplicatesCollapse() {
        val file = configFile()
        val parsed = AvailableCommand.MOVE_DATA.parse(
            arrayOf("move-data", "--include-tables=orders,orders,events", "--exclude-tables=trash,garbage,trash", file.absolutePath)
        )

        assertInstanceOf<Command.Move>(parsed)
        assertEquals(setOf("orders", "events"), parsed.includeTables)
        assertEquals(setOf("trash", "garbage"), parsed.excludeTables)
    }

    @Test
    fun testMoveData_Parse_NoFilesThrows() {
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.MOVE_DATA.parse(arrayOf("move-data"))
        }
        assertTrue(ex.message.contains("No cluster config files"), ex.message)
        assertTrue(ex.message.contains("move-data"), ex.message)
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
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.FINALIZE_MIGRATION.parse(arrayOf("finalize-migration"))
        }
        assertTrue(ex.message.contains("No cluster config files"), ex.message)
        assertTrue(ex.message.contains("finalize-migration"), ex.message)
    }

    @Test
    fun testFinalizeMigration_Parse_RejectsUnknownFlag() {
        val file = configFile()
        val ex = assertThrows<InvalidConfigurationException> {
            AvailableCommand.FINALIZE_MIGRATION.parse(
                arrayOf("finalize-migration", "--target=x", file.absolutePath)
            )
        }
        assertTrue(ex.message.contains("Unknown flag"), ex.message)
    }

    // -------------------- Per-command meta --------------------

    private fun configFile(content: String = "node-01 host=h1 dbname=db\n"): File {
        val file = File.createTempFile("kolbasa-available-command-", null)
        file.writeText(content)
        file.deleteOnExit()
        return file
    }
}
