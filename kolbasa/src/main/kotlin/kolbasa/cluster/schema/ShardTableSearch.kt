package kolbasa.cluster.schema

import kolbasa.cluster.Shard
import kolbasa.producer.ShardId
import kolbasa.schema.Node
import javax.sql.DataSource

/**
 * What [ShardSchema.findShardTable] found on the nodes of a cluster
 */
internal sealed interface ShardTableSearch {

    /**
     * The first node, in node order, whose shard table has every shard
     */
    data class Complete(
        val node: Node,
        val dataSource: DataSource,
        val shards: Map<ShardId, Shard>
    ) : ShardTableSearch

    /**
     * No node has every shard. This is the first node, in node order, that has a shard table at all
     */
    data class Incomplete(
        val node: Node,
        val dataSource: DataSource,
        val shardCount: Int
    ) : ShardTableSearch

    /**
     * No node has a shard table, so the cluster has no shard map yet
     */
    data object Missing : ShardTableSearch
}
