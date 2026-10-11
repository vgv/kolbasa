package kolbasa.butcher.cli.config

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
                "node-01" to mapOf("host" to "h1", "dbname" to "db"),
                "node-02" to mapOf("host" to "h2", "dbname" to "db"),
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
                "node-01" to mapOf(
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
            mapOf("node-01" to mapOf("host" to "h2", "dbname" to "db")),
            merged
        )
    }

    @Test
    fun testMergeNodeFiles_AddsNewNodesFromLaterFiles() {
        val first = writeFile("node-01 host=h1 dbname=db\n")
        val second = writeFile("node-02 host=h2 dbname=db\n")

        val merged = ConfigHelper.mergeNodeFiles(listOf(first, second))
        assertEquals(
            setOf("node-01", "node-02"),
            merged.keys
        )
    }


    @Test
    fun testParseArgs_OnlyFiles() {
        val file = writeFile("")
        val parsed = ConfigHelper.parseArgs(listOf(file.absolutePath), supportedFlags = emptySet())

        assertEquals(1, parsed.files.size)
        assertEquals(file.absolutePath, parsed.files[0].absolutePath)
        assertEquals(emptyMap<String, String>(), parsed.flags)
    }

    @Test
    fun testParseArgs_MultipleFilesInOrder() {
        val a = writeFile("")
        val b = writeFile("")
        val parsed = ConfigHelper.parseArgs(
            listOf(a.absolutePath, b.absolutePath),
            supportedFlags = emptySet()
        )

        assertEquals(
            listOf(a.absolutePath, b.absolutePath),
            parsed.files.map { it.absolutePath }
        )
    }

    @Test
    fun testParseArgs_FlagsAndFiles() {
        val file = writeFile("")
        val parsed = ConfigHelper.parseArgs(
            listOf("--target=node-01", "--shards=0,1,2", file.absolutePath),
            supportedFlags = setOf("--target", "--shards")
        )

        assertEquals(mapOf("--target" to "node-01", "--shards" to "0,1,2"), parsed.flags)
        assertEquals(1, parsed.files.size)
    }

    @Test
    fun testParseArgs_FlagValueCanBeEmpty() {
        val file = writeFile("")
        val parsed = ConfigHelper.parseArgs(
            listOf("--tables=", file.absolutePath),
            supportedFlags = setOf("--tables")
        )

        assertEquals(mapOf("--tables" to ""), parsed.flags)
    }

    @Test
    fun testParseArgs_FlagValueWithEmbeddedEqualsKeepsEverythingAfterFirstEquals() {
        val file = writeFile("")
        val parsed = ConfigHelper.parseArgs(
            listOf("--filter=a=b", file.absolutePath),
            supportedFlags = setOf("--filter")
        )

        assertEquals(mapOf("--filter" to "a=b"), parsed.flags)
    }

    @Test
    fun testParseArgs_FlagWithoutEqualsThrows() {
        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseArgs(listOf("--target"), supportedFlags = setOf("--target"))
        }
        assertTrue(ex.message.contains("--key=value"), ex.message)
        assertTrue(ex.message.contains("--target"), ex.message)
    }

    @Test
    fun testParseArgs_UnknownFlagThrows() {
        val file = writeFile("")
        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseArgs(
                listOf("--wrong=value", file.absolutePath),
                supportedFlags = setOf("--target")
            )
        }
        assertTrue(ex.message.contains("Unknown flag"), ex.message)
        assertTrue(ex.message.contains("--wrong"), ex.message)
    }

    @Test
    fun testParseArgs_DuplicateFlagThrows() {
        val file = writeFile("")
        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseArgs(
                listOf("--target=a", "--target=b", file.absolutePath),
                supportedFlags = setOf("--target")
            )
        }
        assertTrue(ex.message.contains("Duplicate flag"), ex.message)
        assertTrue(ex.message.contains("--target"), ex.message)
    }

    @Test
    fun testParseArgs_UnreadableFileThrows() {
        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseArgs(
                listOf("/definitely/not/a/real/path/at/all-xyz-12345"),
                supportedFlags = emptySet()
            )
        }
        assertTrue(ex.message.contains("Cannot read config file"), ex.message)
    }

    @Test
    fun testParseArgs_NoFilesThrows() {
        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseArgs(
                listOf("--target=node-01"),
                supportedFlags = setOf("--target")
            )
        }
        assertTrue(ex.message.contains("No cluster config files"), ex.message)
    }

    @Test
    fun testParseArgs_EmptyArgsThrows() {
        val ex = assertThrows<InvalidConfigurationException> {
            ConfigHelper.parseArgs(emptyList(), supportedFlags = emptySet())
        }
        assertTrue(ex.message.contains("No cluster config files"), ex.message)
    }

    private fun writeFile(content: String): File {
        val file = File.createTempFile("cluster-node-test-", null)
        file.writeText(content)
        file.deleteOnExit()
        return file
    }

}
