package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File

class ConfigHelperTest {

    @Test
    fun testParseClusterFile_Empty() {
        val file = writeFile("")

        assertEquals(emptyMap<String, Map<String, String>>(), ConfigHelper.parseClusterFile(file))
    }

    @Test
    fun testParseClusterFile_BlankAndCommentLinesIgnored() {
        val file = writeFile(
            """
            # Comment at the top

               # Indented comment

            node-01 host=h1 dbname=db1
            # Comment after a node

            """.trimIndent()
        )

        val parsed = ConfigHelper.parseClusterFile(file)
        assertEquals(1, parsed.size)
        assertEquals(mapOf("host" to "h1", "dbname" to "db1"), parsed["node-01"])
    }

    @Test
    fun testParseClusterFile_MultipleNodes() {
        val file = writeFile(
            """
            node-01 host=h1 port=5432 dbname=orders
            node-02 host=h2 port=5433 dbname=orders
            """.trimIndent()
        )

        val parsed = ConfigHelper.parseClusterFile(file)
        Assertions.assertEquals(2, parsed.size)
        assertEquals(
            mapOf("host" to "h1", "port" to "5432", "dbname" to "orders"),
            parsed["node-01"]
        )
        assertEquals(
            mapOf("host" to "h2", "port" to "5433", "dbname" to "orders"),
            parsed["node-02"]
        )
    }

    @Test
    fun testParseNodeFile_ClusterIdWithoutLibpqValues() {
        val file = writeFile("node-01\nnode-02  \n")
        val parsed = ConfigHelper.parseClusterFile(file)
        assertEquals(
            mapOf<String, Map<String, String>>("node-01" to emptyMap(), "node-02" to emptyMap()),
            parsed
        )
    }

    @Test
    fun testParseClusterFile_DuplicateIdThrows() {
        val file = writeFile(
            """
            node-01 host=h1 dbname=db1
            node-01 host=h2 dbname=db2
            """.trimIndent()
        )

        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            ConfigHelper.parseClusterFile(file)
        }
        assertTrue(ex.messageToShow.contains("node-01"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("Duplicate"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("line 2"), ex.messageToShow)
    }

    @Test
    fun testParseNodeFile_InvalidClusterIdThrows() {
        val file = writeFile("node@01 host=h1 dbname=db1\n")
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            ConfigHelper.parseClusterFile(file)
        }

        assertTrue(ex.messageToShow.contains("Invalid node id"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("node@01"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("line 1"), ex.messageToShow)
    }

    @Test
    fun testParseClusterFile_AllowedIdCharacters() {
        val file = writeFile(
            """
            node-01 host=h1 dbname=db
            node_02 host=h2 dbname=db
            Node.03 host=h3 dbname=db
            NODE04 host=h4 dbname=db
            """.trimIndent()
        )

        val parsed = ConfigHelper.parseClusterFile(file)
        assertEquals(
            setOf("node-01", "node_02", "Node.03", "NODE04"),
            parsed.keys
        )
    }

    @Test
    fun testParseClusterFile_LinesAreTrimmed() {
        val file = writeFile("   node-01   host=h1  dbname=db   \n")
        val parsed = ConfigHelper.parseClusterFile(file)
        assertEquals(
            mapOf("node-01" to mapOf("host" to "h1", "dbname" to "db")),
            parsed
        )
    }

    @Test
    fun testParseNodeFile_LibpqParseError_Propagates() {
        val file = writeFile("node-01 host=h1 dbname='unterminated\n")
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            ConfigHelper.parseClusterFile(file)
        }

        assertTrue(ex.messageToShow.contains("Unterminated"), ex.messageToShow)
    }

    private fun writeFile(content: String): File {
        val file = File.createTempFile("cluster-node-test-", null)
        file.writeText(content)
        file.deleteOnExit()
        return file
    }

}
