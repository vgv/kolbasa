package kolbasa.cluster.butcher

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File

class ConfigTest {

    @Test
    fun testParseRawConfig_NoFile() {
        assertThrows<ButcherException.InvalidConfigurationException> {
            parseRawConfig(File("non-existent-file-${System.nanoTime()}.conf"))
        }
    }

    @Test
    fun testParseRawConfig_EmptyFile() {
        val result = parseRawConfig(prepareConfigFile(""))
        assertTrue(result.isEmpty(), "Expected empty map, got $result")
    }

    @Test
    fun testParseRawConfig_OnlyCommentsAndBlankLines() {
        val content = """
            # this is a comment

            # another comment

        """.trimIndent()
        val result = parseRawConfig(prepareConfigFile(content))
        assertTrue(result.isEmpty(), "Expected empty map, got $result")
    }

    @Test
    fun testParseRawConfig_SingleKeyValue() {
        val result = parseRawConfig(prepareConfigFile("target=node1"))
        assertEquals(mapOf("target" to listOf("node1")), result)
    }

    @Test
    fun testParseRawConfig_MultipleDifferentKeys() {
        val content = """
            target=node1
            shards=0,1,2
            tables=a,b
        """.trimIndent()
        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(
            mapOf(
                "target" to listOf("node1"),
                "shards" to listOf("0,1,2"),
                "tables" to listOf("a,b"),
            ),
            result
        )
    }

    @Test
    fun testParseRawConfig_RepeatedKeyPreservesOrder() {
        val content = """
            node=postgresql://host1/db
            node=postgresql://host2/db
            node=postgresql://host3/db
        """.trimIndent()
        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(
            mapOf(
                "node" to listOf(
                    "postgresql://host1/db",
                    "postgresql://host2/db",
                    "postgresql://host3/db",
                )
            ),
            result
        )
    }

    @Test
    fun testParseRawConfig_CommentsAreIgnored() {
        val content = """
            # leading comment
            target=node1
            # comment between entries
            shards=0,1,2
            # trailing comment
        """.trimIndent()
        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(
            mapOf(
                "target" to listOf("node1"),
                "shards" to listOf("0,1,2"),
            ),
            result
        )
    }

    @Test
    fun testParseRawConfig_BlankLinesAreIgnored() {
        val content = """
            target=node1


            shards=0,1,2

        """.trimIndent()
        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(
            mapOf(
                "target" to listOf("node1"),
                "shards" to listOf("0,1,2"),
            ),
            result
        )
    }

    @Test
    fun testParseRawConfig_WhitespaceAroundKeyAndValueIsTrimmed() {
        val content = """
              target  =   node1
                shards =  0,1,2
        """.trimIndent()
        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(
            mapOf(
                "target" to listOf("node1"),
                "shards" to listOf("0,1,2"),
            ),
            result
        )
    }

    @Test
    fun testParseRawConfig_WhitespaceOnlyLinesAreIgnored() {
        // No multi-line string here: trimIndent() would strip the leading whitespace
        // from the whitespace-only lines, defeating the purpose of the test. Using
        // an explicit string with \n, spaces and \t keeps the whitespace visible and intact.
        // After trim() these lines become empty, so they should be filtered out.
        val content = "target=node1\n   \n\t\nshards=0,1,2\n"
        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(
            mapOf(
                "target" to listOf("node1"),
                "shards" to listOf("0,1,2"),
            ),
            result
        )
    }

    @Test
    fun testParseRawConfig_EqualsSignInValueIsPreserved() {
        // limit=2 in split() means only the first '=' is used as separator;
        // any further '=' chars belong to the value (e.g. URI query strings)
        val content = """
            node=postgresql://host/db?sslmode=require
        """.trimIndent()

        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(
            mapOf("node" to listOf("postgresql://host/db?sslmode=require")),
            result
        )
    }

    @Test
    fun testParseRawConfig_RepeatedKeyMixedWithOtherKeys() {
        val content = """
            node=postgresql://host1/db
            target=node-id
            node=postgresql://host2/db
            shards=0,1,2
            node=postgresql://host3/db
        """.trimIndent()
        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(
            mapOf(
                "node" to listOf(
                    "postgresql://host1/db",
                    "postgresql://host2/db",
                    "postgresql://host3/db",
                ),
                "target" to listOf("node-id"),
                "shards" to listOf("0,1,2"),
            ),
            result
        )
    }

    @Test
    fun testParseRawConfig_EmptyValue() {
        val content = """
            key=
        """.trimIndent()

        val result = parseRawConfig(prepareConfigFile(content))
        assertEquals(mapOf("key" to listOf("")), result)
    }

    private fun prepareConfigFile(content: String): File {
        val file = File.createTempFile("kolbasa-butcher-temp-conf-", ".conf")
        file.deleteOnExit()
        file.writeText(content)
        return file
    }
}
