package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import kolbasa.cluster.butcher.HumanReadableDataSource
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import javax.sql.DataSource
import kotlin.collections.iterator

internal data class ClusterNodes(
    val dataSources: List<DataSource>
) {

    companion object {

        private val NODE_ID_REGEX = Regex("[a-zA-Z0-9_.-]+")

        /**
         * Parse + merge all config files, then build [ClusterNodes]
         */
        internal fun buildClusterNodes(files: List<File>): ClusterNodes {
            val merged = mergeNodeFiles(files)
            if (merged.isEmpty()) {
                throw ButcherException.InvalidConfigurationException("No nodes defined in config files")
            }

            val dataSources = merged.map { (id, libpqValues) ->
                val pgEndpoint = PgEndpoint.parsePgEndpoint(id, libpqValues)
                createDataSource(pgEndpoint)
            }

            return ClusterNodes(dataSources)
        }

        /**
         * Parse a single node-config file into a map of `id -> libpq pairs`.
         *
         * Line format: `<id> <libpq key=value pairs>`. Blank lines and `#` comments skipped.
         * Duplicate id within one file is an error.
         */
        internal fun parseNodeFile(file: File): Map<String, Map<String, String>> {
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
        private fun mergeNodeFiles(files: List<File>): Map<String, Map<String, String>> {
            val result = mutableMapOf<String, MutableMap<String, String>>()

            for (file in files) {
                for ((id, libpqValues) in parseNodeFile(file)) {
                    val currentValues = result.computeIfAbsent(id) { mutableMapOf() }
                    currentValues.putAll(libpqValues)
                }
            }

            return result
        }

        private fun createDataSource(endpoint: PgEndpoint): DataSource {
            val dataSource = PGSimpleDataSource().apply {
                serverNames = arrayOf(endpoint.host)
                portNumbers = intArrayOf(endpoint.port ?: 5432)
                databaseName = endpoint.dbName
                endpoint.user?.let { this.user = it }
                endpoint.password?.let { this.password = it }
                endpoint.schema?.let { this.currentSchema = it }
            }

            return HumanReadableDataSource(dataSource, endpoint)
        }
    }
}
