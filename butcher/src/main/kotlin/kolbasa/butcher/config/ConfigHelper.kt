package kolbasa.butcher.config

import java.io.File

internal object ConfigHelper {

    private val NODE_ID_REGEX = Regex("[a-zA-Z0-9_.-]+")


    /**
     * Parse a single node-config file into a map of `id -> libpq pairs`.
     *
     * Line format: `<id> <libpq key=value pairs>`. Blank lines and `#` comments skipped.
     * Duplicate id within one file is an error.
     */
    internal fun parseClusterFile(file: File): Map<String, Map<String, String>> {
        val result = mutableMapOf<String, Map<String, String>>()

        file.readLines().forEachIndexed { index, rawLine ->
            val lineNum = index + 1
            val line = rawLine.trim()
            if (line.isEmpty() || line.startsWith("#")) return@forEachIndexed

            val firstWhitespace = line.indexOfFirst { it.isWhitespace() }
            val (id, rest) = if (firstWhitespace < 0) {
                line to ""
            } else {
                line.substring(0, firstWhitespace) to line.substring(firstWhitespace).trim()
            }

            if (!NODE_ID_REGEX.matches(id)) {
                throw InvalidConfigurationException(
                    "Invalid node id '$id' in ${file.name} line $lineNum. Must match ${NODE_ID_REGEX.pattern}"
                )
            }

            if (result.containsKey(id)) {
                throw InvalidConfigurationException("Duplicate node id '$id' in ${file.name} line $lineNum")
            }

            val libpqValues = if (rest.isEmpty()) emptyMap() else LibpqTokenizer.tokenize(rest)
            result[id] = libpqValues
        }

        return result
    }

    /**
     * Merge per-file maps `id -> pairs` into one combined map.
     *
     * Union of keys per id across files. In case of duplicated keys, last file wins.
     */
    internal fun mergeNodeFiles(files: List<File>): Map<String, Map<String, String>> {
        val result = mutableMapOf<String, MutableMap<String, String>>()

        for (file in files) {
            for ((id, libpqValues) in parseClusterFile(file)) {
                val currentValues = result.computeIfAbsent(id) { mutableMapOf() }
                currentValues.putAll(libpqValues)
            }
        }

        return result
    }
}
