package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import kolbasa.cluster.butcher.HumanReadableDataSource
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import javax.sql.DataSource

internal data class ClusterNodes(
    val dataSources: List<DataSource>
) {

    companion object {

        /**
         * Parse + merge all config files, then build [ClusterNodes]
         */
        internal fun buildClusterNodes(files: List<File>): ClusterNodes {
            val merged = ConfigHelper.mergeNodeFiles(files)
            if (merged.isEmpty()) {
                throw ButcherException.InvalidConfigurationException("No nodes defined in config files")
            }

            val dataSources = merged.map { (id, libpqValues) ->
                val pgEndpoint = PgEndpoint.parsePgEndpoint(id, libpqValues)
                createDataSource(pgEndpoint)
            }

            return ClusterNodes(dataSources)
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
