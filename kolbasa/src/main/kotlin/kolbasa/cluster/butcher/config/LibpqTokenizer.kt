package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException

/**
 * Cursor-based parser for libpq `key=value` connection strings.
 *
 * Splits a libpq-style line into ordered `(key, value)` pairs. A duplicate key
 * silently wins last, matching libpq's behavior.
 *
 * Grammar (simplified):
 *   line  := pair (whitespace pair)*  whitespace*
 *   pair  := key whitespace* '=' whitespace* value
 *   key   := [^\s=]+
 *   value := quotedValue | unquotedValue
 *   unquotedValue := [^\s]*
 *   quotedValue   := "'" ( escaped | [^'\\] )* "'"
 *   escaped       := "\\'" | "\\\\"
 *
 * Reference: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
 */
internal class LibpqTokenizer(private val line: String) {

    private var cursor: Int = 0

    fun tokenize(): Map<String, String> {
        val result = mutableMapOf<String, String>()

        skipWhitespace()
        while (!atEnd()) {
            val key = readKey()
            skipWhitespace()
            expectEquals(key)
            skipWhitespace()
            val value = readValue(key)
            result[key] = value
            skipWhitespace()
        }

        return result
    }

    private fun atEnd(): Boolean = cursor >= line.length

    private fun peek(): Char = line[cursor]

    private fun skipWhitespace() {
        while (!atEnd() && peek().isWhitespace()) cursor++
    }

    private fun readKey(): String {
        val keyStart = cursor
        while (!atEnd() && !peek().isWhitespace() && peek() != '=') cursor++

        val key = line.substring(keyStart, cursor)
        if (key.isEmpty()) {
            throw ButcherException.InvalidConfigurationException("Empty key at position $keyStart in '$line'")
        }
        return key
    }

    private fun expectEquals(key: String) {
        if (atEnd() || peek() != '=') {
            throw ButcherException.InvalidConfigurationException("Expected '=' after key '$key' in '$line'")
        }
        cursor++ // consume '='
    }

    private fun readValue(key: String): String {
        return if (!atEnd() && peek() == '\'') readQuotedValue(key) else readUnquotedValue()
    }

    private fun readUnquotedValue(): String {
        val valueStart = cursor
        while (!atEnd() && !peek().isWhitespace()) cursor++
        return line.substring(valueStart, cursor)
    }

    private fun readQuotedValue(key: String): String {
        cursor++ // consume opening quote

        val buffer = StringBuilder()
        while (!atEnd()) {
            val c = peek()
            when {
                c == '\'' -> {
                    cursor++ // consume closing quote
                    return buffer.toString()
                }

                c == '\\' && isEscapable(nextOrNull()) -> {
                    buffer.append(line[cursor + 1])
                    cursor += 2
                }

                else -> {
                    buffer.append(c)
                    cursor++
                }
            }
        }

        throw ButcherException.InvalidConfigurationException("Unterminated quoted value for key '$key' in '$line'")
    }

    private fun nextOrNull(): Char? = if (cursor + 1 < line.length) line[cursor + 1] else null

    private fun isEscapable(c: Char?): Boolean = c == '\'' || c == '\\'

    companion object {

        fun tokenize(line: String): Map<String, String> = LibpqTokenizer(line).tokenize()

    }
}
