package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
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
                throw ButcherException.InvalidConfigurationException(
                    "Invalid node id '$id' in ${file.name} line $lineNum. Must match ${NODE_ID_REGEX.pattern}"
                )
            }

            if (result.containsKey(id)) {
                throw ButcherException.InvalidConfigurationException("Duplicate node id '$id' in ${file.name} line $lineNum")
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

    /**
     * Extract flags (`--key=value`) and positional file paths from [args].
     *
     * Rules:
     *  - `--key=value` → flag; key must be in [supportedFlags].
     *  - `--key` alone (no `=`) → error.
     *  - Anything else → file path; must be readable.
     *  - Duplicate flag → error.
     *  - Zero files → error.
     */
    internal fun parseArgs(args: List<String>, supportedFlags: Set<String>): ParsedArgs {
        val files = mutableListOf<File>()
        val flags = mutableMapOf<String, String>()

        for (arg in args) {
            if (arg.startsWith("--")) {
                // this is an argument, like --shards or --target
                val eq = arg.indexOf('=')
                if (eq < 0) {
                    throw ButcherException.InvalidConfigurationException("Flag '$arg' must be in '--key=value' form")
                }

                val key = arg.substring(0, eq)
                val value = arg.substring(eq + 1)
                if (key !in supportedFlags) {
                    throw ButcherException.InvalidConfigurationException("Unknown flag '$key'. Supported flags: $supportedFlags")
                }
                if (key in flags) {
                    throw ButcherException.InvalidConfigurationException("Duplicate flag '$key'")
                }

                flags[key] = value
            } else {
                // this is a cluster file, one of
                val file = File(arg)
                if (!file.canRead()) {
                    throw ButcherException.InvalidConfigurationException("Cannot read config file: ${file.absolutePath}")
                }
                files.add(file)
            }
        }

        if (files.isEmpty()) {
            throw ButcherException.InvalidConfigurationException("No cluster config files provided. At least one config file is required.")
        }

        return ParsedArgs(files, flags)
    }


}

/**
 * Parsed CLI args split into flags and positional file paths.
 */
internal data class ParsedArgs(val files: List<File>, val flags: Map<String, String>)

