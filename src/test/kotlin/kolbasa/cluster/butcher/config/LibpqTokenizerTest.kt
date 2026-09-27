package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LibpqTokenizerTest {

    @Test
    fun testTokenize_EmptyString() {
        val expected = emptyMap<String, String>()
        val actual = LibpqTokenizer.tokenize("")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_OnlyWhitespace() {
        val expected = emptyMap<String, String>()
        val actual = LibpqTokenizer.tokenize("   \t  ")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_SinglePair() {
        val expected = mapOf("host" to "db1")
        val actual = LibpqTokenizer.tokenize("host=db1")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_MultiplePairs() {
        val expected = mapOf("host" to "db1", "port" to "5432", "dbname" to "orders")
        val actual = LibpqTokenizer.tokenize("host=db1 port=5432 dbname=orders")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_LastDuplicateWins() {
        val expected = mapOf("host" to "db5")
        val actual = LibpqTokenizer.tokenize("host=db1 host=db2 host=db3 host=db4 host=db5")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_WhitespaceAroundEquals() {
        val expected = mapOf("host" to "db1", "port" to "5432")
        val actual = LibpqTokenizer.tokenize("host = db1   port= 5432")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_TabsAndMultipleSpacesBetweenPairs() {
        val expected = mapOf("host" to "db1", "port" to "5432", "dbname" to "orders")
        val actual = LibpqTokenizer.tokenize("host=db1 \t  port=5432\tdbname=orders")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_LeadingAndTrailingWhitespace() {
        val expected = mapOf("host" to "db1")
        val actual = LibpqTokenizer.tokenize("   host=db1   ")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_QuotedValueWithSpaces() {
        val expected = mapOf("password" to "p@ss word")
        val actual = LibpqTokenizer.tokenize("password='p@ss word'")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_QuotedValueWithEscapedQuote() {
        val expected = mapOf("password" to "it's")
        val actual = LibpqTokenizer.tokenize("""password='it\'s'""")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_QuotedValueWithEscapedBackslash() {
        val expected = mapOf("password" to """a\b""")
        val actual = LibpqTokenizer.tokenize("""password='a\\b'""")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_EmptyQuotedValue() {
        val expected = mapOf("password" to "")
        val actual = LibpqTokenizer.tokenize("password=''")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_UnrecognizedEscapesAreKeptLiteral() {
        // libpq: only \' and \\ are recognized inside quotes; \n stays as backslash + n.
        val expected = mapOf("k" to """a\nb""")
        val actual = LibpqTokenizer.tokenize("""k='a\nb'""")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_MixQuotedAndUnquoted() {
        val expected = mapOf("host" to "db1", "password" to "p ss", "port" to "5432")
        val actual = LibpqTokenizer.tokenize("host=db1 password='p ss' port=5432")

        assertEquals(expected, actual)
    }

    @Test
    fun testTokenize_UnterminatedQuoteThrows() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            LibpqTokenizer.tokenize("password='abc")
        }

        assertTrue(ex.messageToShow.contains("Unterminated"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("password"), ex.messageToShow)
    }

    @Test
    fun testTokenize_MissingEqualsThrows() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            LibpqTokenizer.tokenize("host db1")
        }
        assertTrue(ex.messageToShow.contains("Expected '='"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("host"), ex.messageToShow)
    }

    @Test
    fun testTokenize_KeyWithoutValueThrows() {
        // "host=" with nothing after: unquoted value is empty — legal at end of line.
        val expected = mapOf("host" to "")
        val actual = LibpqTokenizer.tokenize("host=")

        assertEquals(expected, actual)
    }
}
