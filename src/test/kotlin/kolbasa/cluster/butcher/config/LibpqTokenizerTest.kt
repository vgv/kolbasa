package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LibpqTokenizerTest {

    @Test
    fun testTokenize_EmptyString() {
        Assertions.assertEquals(emptyMap<String, String>(), LibpqTokenizer.tokenize(""))
    }

    @Test
    fun testTokenize_OnlyWhitespace() {
        Assertions.assertEquals(emptyMap<String, String>(), LibpqTokenizer.tokenize("   \t  "))
    }

    @Test
    fun testTokenize_SinglePair() {
        Assertions.assertEquals(
            mapOf("host" to "db1"),
            LibpqTokenizer.tokenize("host=db1")
        )
    }

    @Test
    fun testTokenize_MultiplePairs() {
        Assertions.assertEquals(
            mapOf("host" to "db1", "port" to "5432", "dbname" to "orders"),
            LibpqTokenizer.tokenize("host=db1 port=5432 dbname=orders")
        )
    }

    @Test
    fun testTokenize_LastDuplicateWins() {
        Assertions.assertEquals(
            mapOf("host" to "db5"),
            LibpqTokenizer.tokenize("host=db1 host=db2 host=db3 host=db4 host=db5")
        )
    }

    @Test
    fun testTokenize_WhitespaceAroundEquals() {
        Assertions.assertEquals(
            mapOf("host" to "db1", "port" to "5432"),
            LibpqTokenizer.tokenize("host = db1   port= 5432")
        )
    }

    @Test
    fun testTokenize_TabsAndMultipleSpacesBetweenPairs() {
        Assertions.assertEquals(
            mapOf("host" to "db1", "port" to "5432", "dbname" to "orders"),
            LibpqTokenizer.tokenize("host=db1 \t  port=5432\tdbname=orders")
        )
    }

    @Test
    fun testTokenize_LeadingAndTrailingWhitespace() {
        Assertions.assertEquals(
            mapOf("host" to "db1"),
            LibpqTokenizer.tokenize("   host=db1   ")
        )
    }

    @Test
    fun testTokenize_QuotedValueWithSpaces() {
        Assertions.assertEquals(
            mapOf("password" to "p@ss word"),
            LibpqTokenizer.tokenize("password='p@ss word'")
        )
    }

    @Test
    fun testTokenize_QuotedValueWithEscapedQuote() {
        Assertions.assertEquals(
            mapOf("password" to "it's"),
            LibpqTokenizer.tokenize("""password='it\'s'""")
        )
    }

    @Test
    fun testTokenize_QuotedValueWithEscapedBackslash() {
        Assertions.assertEquals(
            mapOf("password" to """a\b"""),
            LibpqTokenizer.tokenize("""password='a\\b'""")
        )
    }

    @Test
    fun testTokenize_EmptyQuotedValue() {
        Assertions.assertEquals(
            mapOf("password" to ""),
            LibpqTokenizer.tokenize("password=''")
        )
    }

    @Test
    fun testTokenize_UnrecognizedEscapesAreKeptLiteral() {
        // libpq: only \' and \\ are recognized inside quotes; \n stays as backslash + n.
        Assertions.assertEquals(
            mapOf("k" to """a\nb"""),
            LibpqTokenizer.tokenize("""k='a\nb'""")
        )
    }

    @Test
    fun testTokenize_MixQuotedAndUnquoted() {
        Assertions.assertEquals(
            mapOf("host" to "db1", "password" to "p ss", "port" to "5432"),
            LibpqTokenizer.tokenize("host=db1 password='p ss' port=5432")
        )
    }

    @Test
    fun testTokenize_UnterminatedQuoteThrows() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            LibpqTokenizer.tokenize("password='abc")
        }
        Assertions.assertTrue(ex.messageToShow.contains("Unterminated"), ex.messageToShow)
        Assertions.assertTrue(ex.messageToShow.contains("password"), ex.messageToShow)
    }

    @Test
    fun testTokenize_MissingEqualsThrows() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            LibpqTokenizer.tokenize("host db1")
        }
        Assertions.assertTrue(ex.messageToShow.contains("Expected '='"), ex.messageToShow)
        Assertions.assertTrue(ex.messageToShow.contains("host"), ex.messageToShow)
    }

    @Test
    fun testTokenize_KeyWithoutValueThrows() {
        // "host=" with nothing after: unquoted value is empty — legal at end of line.
        Assertions.assertEquals(
            mapOf("host" to ""),
            LibpqTokenizer.tokenize("host=")
        )
    }
}
