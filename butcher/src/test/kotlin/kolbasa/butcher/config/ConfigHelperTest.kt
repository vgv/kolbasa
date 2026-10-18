package kolbasa.butcher.config

import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File

class ConfigHelperTest {

    @Test
    fun testParseClusterFile_Empty() {
        val file = writeFile("")

        assertEquals(emptyMap<NodeId, Map<String, String>>(), ConfigHelper.parseClusterFile(file))
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
        assertEquals(mapOf("host" to "h1", "dbname" to "db1"), parsed[NodeId("node-01")])
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
            parsed[NodeId("node-01")]
        )
        assertEquals(
            mapOf("host" to "h2", "port" to "5433", "dbname" to "orders"),
            parsed[NodeId("node-02")]
        )
    }

    @Test
    fun testParseNodeFile_ClusterIdWithoutLibpqValues() {
        val file = writeFile("node-01\nnode-02  \n")
        val parsed = ConfigHelper.parseClusterFile(file)
        assertEquals(
            mapOf<NodeId, Map<String, String>>(NodeId("node-01") to emptyMap(), NodeId("node-02") to emptyMap()),
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

        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseClusterFile(file)
        }
        assertTrue(ex.message.contains("node-01"), ex.message)
        assertTrue(ex.message.contains("Duplicate"), ex.message)
        assertTrue(ex.message.contains("line 2"), ex.message)
    }

    @Test
    fun testParseNodeFile_InvalidClusterIdThrows() {
        val file = writeFile("node@01 host=h1 dbname=db1\n")
        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseClusterFile(file)
        }

        assertTrue(ex.message.contains("Invalid node id"), ex.message)
        assertTrue(ex.message.contains("node@01"), ex.message)
        assertTrue(ex.message.contains("line 1"), ex.message)
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
            setOf(NodeId("node-01"), NodeId("node_02"), NodeId("Node.03"), NodeId("NODE04")),
            parsed.keys
        )
    }

    @Test
    fun testParseClusterFile_IdLengthLimit() {
        val longest = "n".repeat(100)
        assertEquals(setOf(NodeId(longest)), ConfigHelper.parseClusterFile(writeFile("$longest host=h1 dbname=db\n")).keys)

        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseClusterFile(writeFile("${longest}n host=h1 dbname=db\n"))
        }
        assertTrue(ex.message.contains("Invalid node id"), ex.message)
    }

    @Test
    fun testParseClusterFile_LinesAreTrimmed() {
        val file = writeFile("   node-01   host=h1  dbname=db   \n")
        val parsed = ConfigHelper.parseClusterFile(file)
        assertEquals(
            mapOf(NodeId("node-01") to mapOf("host" to "h1", "dbname" to "db")),
            parsed
        )
    }

    @Test
    fun testParseNodeFile_LibpqParseError_Propagates() {
        val file = writeFile("node-01 host=h1 dbname='unterminated\n")
        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseClusterFile(file)
        }

        assertTrue(ex.message.contains("Unterminated"), ex.message)
    }

    @Test
    fun testMergeNodeFiles_SingleFile() {
        val file = writeFile(
            """
            node-01 host=h1 dbname=db
            node-02 host=h2 dbname=db
            """.trimIndent()
        )

        val merged = ConfigHelper.mergeNodeFiles(listOf(file))
        assertEquals(
            mapOf(
                NodeId("node-01") to mapOf("host" to "h1", "dbname" to "db"),
                NodeId("node-02") to mapOf("host" to "h2", "dbname" to "db"),
            ),
            merged
        )
    }

    @Test
    fun testMergeNodeFiles_UnionKeysAcrossFiles() {
        val topology = writeFile("node-01 host=h1 port=5432 dbname=db\n")
        val secrets = writeFile("node-01 user=app password=secret\n")

        val merged = ConfigHelper.mergeNodeFiles(listOf(topology, secrets))
        assertEquals(
            mapOf(
                NodeId("node-01") to mapOf(
                    "host" to "h1",
                    "port" to "5432",
                    "dbname" to "db",
                    "user" to "app",
                    "password" to "secret",
                )
            ),
            merged
        )
    }

    @Test
    fun testMergeNodeFiles_LastFileWinsOnDuplicateKey() {
        val first = writeFile("node-01 host=h1 dbname=db\n")
        val second = writeFile("node-01 host=h2\n")

        val merged = ConfigHelper.mergeNodeFiles(listOf(first, second))
        assertEquals(
            mapOf(NodeId("node-01") to mapOf("host" to "h2", "dbname" to "db")),
            merged
        )
    }

    @Test
    fun testMergeNodeFiles_AddsNewNodesFromLaterFiles() {
        val first = writeFile("node-01 host=h1 dbname=db\n")
        val second = writeFile("node-02 host=h2 dbname=db\n")

        val merged = ConfigHelper.mergeNodeFiles(listOf(first, second))
        assertEquals(
            setOf(NodeId("node-01"), NodeId("node-02")),
            merged.keys
        )
    }

    @Test
    fun testEndpoints_OneEndpointPerNode() {
        val topology = writeFile("node-01 host=h1 dbname=db\nnode-02 host=h2 port=5433 dbname=db schema=s2\n")
        val secrets = writeFile("node-01 user=app password=secret\n")

        assertEquals(
            listOf(
                PgEndpoint(
                    NodeId("node-01"), "h1", port = null, dbName = "db", user = "app", password = "secret", schema = null
                ),
                PgEndpoint(
                    NodeId("node-02"), "h2", port = 5433, dbName = "db", user = null, password = null, schema = "s2"
                ),
            ),
            ConfigHelper.endpoints(listOf(topology, secrets))
        )
    }

    @Test
    fun testEndpoints_NoNodes() {
        val file = writeFile("# Nothing but a comment\n")

        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.endpoints(listOf(file))
        }
        assertTrue(ex.message.contains("No nodes defined"), ex.message)
    }

    private fun writeFile(content: String): File {
        val file = File.createTempFile("cluster-node-test-", null)
        file.writeText(content)
        file.deleteOnExit()
        return file
    }
}
