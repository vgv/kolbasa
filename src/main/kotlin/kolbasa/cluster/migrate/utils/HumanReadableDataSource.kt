package kolbasa.cluster.migrate.utils

import javax.sql.DataSource

internal class HumanReadableDataSource(
    private val peer: DataSource,
    private val url: String
) : DataSource by peer {

    override fun toString(): String {
        return "DataSource($url)"
    }
}
