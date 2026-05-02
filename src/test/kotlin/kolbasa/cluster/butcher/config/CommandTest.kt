package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertThrows
import java.io.File

class CommandTest {

    @Test
    fun testParseCommand_Check() {
        val file = configFile()
        val cmd = Command.parseCommand(arrayOf("check-cluster", file.absolutePath))

        assertInstanceOf<Command.Check>(cmd)
        assertEquals(AvailableCommand.CHECK_CLUSTER, cmd.command)
    }

    @Test
    fun testParseCommand_Prepare() {
        val file = configFile()
        val cmd = Command.parseCommand(
            arrayOf("prepare-migration", "--target=node-01", "--shards=0,1", file.absolutePath)
        )

        assertInstanceOf<Command.Prepare>(cmd)
        assertEquals(NodeId("node-01"), cmd.target)
        assertEquals(listOf(0, 1), cmd.shards)
    }

    @Test
    fun testParseCommand_Move() {
        val file = configFile()
        val cmd = Command.parseCommand(arrayOf("move-data", file.absolutePath))

        assertInstanceOf<Command.Move>(cmd)
        assertEquals(AvailableCommand.MOVE_DATA, cmd.command)
    }

    @Test
    fun testParseCommand_Finalize() {
        val file = configFile()
        val cmd = Command.parseCommand(arrayOf("finalize-migration", file.absolutePath))

        assertInstanceOf<Command.Finalize>(cmd)
        assertEquals(AvailableCommand.FINALIZE_MIGRATION, cmd.command)
    }

    @Test
    fun testParseCommand_EmptyArgs_Throws() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            Command.parseCommand(emptyArray())
        }
        // Empty args should produce the global usage, not a command-specific one.
        Assertions.assertTrue(ex.messageToShow.contains("Usage:"), ex.messageToShow)
        Assertions.assertTrue(ex.messageToShow.contains("check-cluster"), ex.messageToShow)
    }

    @Test
    fun testParseCommand_UnknownCommand_Throws() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            Command.parseCommand(arrayOf("do-the-thing", "some-file"))
        }
        Assertions.assertTrue(ex.messageToShow.contains("Unknown command"), ex.messageToShow)
        Assertions.assertTrue(ex.messageToShow.contains("do-the-thing"), ex.messageToShow)
        Assertions.assertTrue(ex.messageToShow.contains("Usage:"), ex.messageToShow)
    }

    @Test
    fun testParseCommand_DispatchedCommandErrorsArePropagated() {
        // move-data without a config file → command-level error, not a Command.parseCommand-level error.
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            Command.parseCommand(arrayOf("move-data"))
        }
        Assertions.assertTrue(ex.messageToShow.contains("No cluster config files"), ex.messageToShow)
    }

    private fun configFile(content: String = "node-01 host=h1 dbname=db\n"): File {
        val file = File.createTempFile("kolbasa-cluster-config-", null)
        file.writeText(content)
        file.deleteOnExit()
        return file
    }

}
