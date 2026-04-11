package kolbasa.cluster.butcher

import kolbasa.cluster.butcher.config.PgEndpoint
import java.sql.ConnectionBuilder
import java.sql.ShardingKeyBuilder
import javax.sql.DataSource

internal class HumanReadableDataSource(
    private val peer: DataSource,
    private val pgEndpoint: PgEndpoint
) : DataSource by peer {

    override fun toString(): String {
        return "DataSource($pgEndpoint)"
    }

    override fun createConnectionBuilder(): ConnectionBuilder? {
        return peer.createConnectionBuilder()
    }

    override fun createShardingKeyBuilder(): ShardingKeyBuilder? {
        return peer.createShardingKeyBuilder()
    }
}
