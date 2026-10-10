package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException

/**
 * Parsed PostgreSQL endpoint, holding all fields required to build a [javax.sql.DataSource].
 *
 * Closed set of fields: when a new option needs supporting (e.g. `sslmode`), it should be
 * added as an explicit typed field rather than a generic property bag.
 *
 * @param id node identifier from the config file (first whitespace-separated token on the line)
 * @param host PostgreSQL host
 * @param port PostgreSQL port, or `null` if not specified (defaults to 5432)
 * @param dbName database name
 * @param user user name, or `null` if absent
 * @param password password, or `null` if absent
 * @param schema optional schema name (butcher-specific key, not a libpq parameter)
 */
internal data class PgEndpoint(
    val id: String,
    val host: String,
    val port: Int?,
    val dbName: String,
    val user: String?,
    val password: String?,
    val schema: String?,
) {
    /** Human-readable identifier: no credentials. */
    override fun toString(): String = "$id ($host:${port ?: 5432}/$dbName)"

    companion object {

        /**
         * Build a [kolbasa.cluster.butcher.config.PgEndpoint] from a node id and its libpq key/value pairs.
         *
         * Known keys: host, port, user, password, dbname, schema. Unknown keys silently ignored.
         * `host` and `dbname` are required.
         */
        internal fun parsePgEndpoint(id: String, libpqValues: Map<String, String>): PgEndpoint {
            val host = libpqValues["host"]
                ?: throw ButcherException.InvalidConfigurationException("'host' is required for node '$id'")

            val dbName = libpqValues["dbname"]
                ?: throw ButcherException.InvalidConfigurationException("'dbname' is required for node '$id'")

            val port = libpqValues["port"]?.let {
                it.toIntOrNull() ?: throw ButcherException.InvalidConfigurationException("Invalid port '$it' for node '$id'")
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
