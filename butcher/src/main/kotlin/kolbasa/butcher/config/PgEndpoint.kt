package kolbasa.butcher.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kolbasa.butcher.HumanReadableDataSource
import kolbasa.schema.NodeId
import org.postgresql.ds.PGSimpleDataSource
import javax.sql.DataSource

/**
 * Parsed PostgreSQL endpoint, holding all fields required to build a [javax.sql.DataSource].
 *
 * Closed set of fields: when a new option needs supporting (e.g. `sslmode`), it should be
 * added as an explicit typed field rather than a generic property bag.
 *
 * @param id the node's id, the first whitespace-separated token on its line
 * @param host PostgreSQL host
 * @param port PostgreSQL port, or `null` if not specified (defaults to 5432)
 * @param dbName database name
 * @param user user name, or `null` if absent
 * @param password password, or `null` if absent
 * @param schema optional schema name (butcher-specific key, not a libpq parameter)
 */
internal data class PgEndpoint(
    val id: NodeId,
    val host: String,
    val port: Int?,
    val dbName: String,
    val user: String?,
    val password: String?,
    val schema: String?,
) {
    /** Human-readable identifier: no credentials. */
    override fun toString(): String = "${id.id} ($host:${port ?: 5432}/$dbName)"

    /**
     * A plain driver data source: a new connection for every request, and a driver error when there is
     * none to be had
     *
     * This is what reachability is checked on. A pool answers the same question slowly and in its own
     * words – it waits out its whole connection timeout and then reports that timeout, not what the
     * driver said.
     */
    internal fun driverDataSource(): DataSource = HumanReadableDataSource(pgDataSource(), this)

    /**
     * The same node, behind a pool: what the work itself runs on
     *
     * `move-data` takes a connection from both sides for every batch of 1000 rows, so a migration of ten
     * million rows asks for twenty thousand connections. Unpooled, every one of them is a new handshake.
     *
     * Nothing closes the pool. The process ends with the command, and Hikari's own threads are daemons.
     */
    internal fun pooledDataSource(): DataSource {
        val config = HikariConfig().apply {
            dataSource = pgDataSource()
            poolName = id.id

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

        return HumanReadableDataSource(HikariDataSource(config), this)
    }

    private fun pgDataSource(): DataSource {
        // Not built with apply: PGSimpleDataSource has a user and a password of its own, which would hide these
        val dataSource = PGSimpleDataSource()
        dataSource.serverNames = arrayOf(host)
        dataSource.portNumbers = intArrayOf(port ?: 5432)
        dataSource.databaseName = dbName
        user?.let { dataSource.user = it }
        password?.let { dataSource.password = it }
        schema?.let { dataSource.currentSchema = it }
        return dataSource
    }

    companion object {

        /**
         * Build a [PgEndpoint] from a node id and its libpq key/value pairs.
         *
         * Known keys: host, port, user, password, dbname, schema. Unknown keys silently ignored.
         * `host` and `dbname` are required.
         */
        internal fun parsePgEndpoint(id: NodeId, libpqValues: Map<String, String>): PgEndpoint {
            val host = libpqValues["host"]
                ?: throw InvalidConfigurationException("'host' is required for node '${id.id}'")

            val dbName = libpqValues["dbname"]
                ?: throw InvalidConfigurationException("'dbname' is required for node '${id.id}'")

            val port = libpqValues["port"]?.let {
                it.toIntOrNull() ?: throw InvalidConfigurationException("Invalid port '$it' for node '${id.id}'")
            }

            return PgEndpoint(
                id = id,
                host = host,
                port = port,
                dbName = dbName,
                user = libpqValues["user"],
                password = libpqValues["password"],
                schema = libpqValues["schema"],
            )
        }
    }

}
