package kolbasa.butcher.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kolbasa.butcher.HumanReadableDataSource
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import javax.sql.DataSource

internal object ClusterNodes {

    /**
     * Parse + merge all config files into one endpoint per node
     */
    internal fun endpoints(files: List<File>): List<PgEndpoint> {
        val merged = ConfigHelper.mergeNodeFiles(files)
        if (merged.isEmpty()) {
            throw InvalidConfigurationException("No nodes defined in config files")
        }

        return merged.map { (id, libpqValues) ->
            PgEndpoint.parsePgEndpoint(id, libpqValues)
        }
    }

    /**
     * A plain driver data source: a new connection for every request, and a driver error when there is
     * none to be had
     *
     * This is what reachability is checked on. A pool answers the same question slowly and in its own
     * words – it waits out its whole connection timeout and then reports that timeout, not what the
     * driver said.
     */
    internal fun driverDataSource(endpoint: PgEndpoint): DataSource =
        HumanReadableDataSource(pgDataSource(endpoint), endpoint)

    /**
     * The same node, behind a pool: what the work itself runs on
     *
     * `move-data` takes a connection from both sides for every batch of 1000 rows, so a migration of ten
     * million rows asks for twenty thousand connections. Unpooled, every one of them is a new handshake.
     *
     * Nothing closes the pool. The process ends with the command, and Hikari's own threads are daemons.
     */
    internal fun pooledDataSource(endpoint: PgEndpoint): DataSource {
        val config = HikariConfig().apply {
            dataSource = pgDataSource(endpoint)
            poolName = endpoint.id

            // Every command works through the nodes one connection at a time; the second is headroom
            maximumPoolSize = 2
            minimumIdle = 0

            // Nothing is opened before the work asks for it. Reachability has been checked by then, on
            // the driver data source above
            initializationFailTimeout = -1

            // Hikari's own default is 30 seconds, which is a long time to wait for a node that died
            // after it answered the reachability check
            connectionTimeout = 3_000
        }

        return HumanReadableDataSource(HikariDataSource(config), endpoint)
    }

    private fun pgDataSource(endpoint: PgEndpoint): DataSource = PGSimpleDataSource().apply {
        serverNames = arrayOf(endpoint.host)
        portNumbers = intArrayOf(endpoint.port ?: 5432)
        databaseName = endpoint.dbName
        endpoint.user?.let { this.user = it }
        endpoint.password?.let { this.password = it }
        endpoint.schema?.let { this.currentSchema = it }
    }
}
