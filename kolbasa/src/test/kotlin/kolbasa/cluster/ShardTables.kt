package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.Node
import javax.sql.DataSource

/**
 * Gives every shard of [shardsToFill] a node of [nodes] the way a new cluster gets its map: evenly, see
 * [ShardSchema.spreadEvenly]
 */
internal fun fillShardTableEvenly(
    dataSource: DataSource,
    nodes: List<Node>,
    shardsToFill: Iterable<Int> = ShardId.SHARDS_RANGE
) {
    val owners = ShardSchema.spreadEvenly(nodes.map { it.id }, emptyList(), shardsToFill.map { ShardId.of(it) })
    ShardSchema.fillShardTable(dataSource, owners)
}

/**
 * Gives every shard of [shardsToFill] a random node of [nodes]. Two calls give two maps that differ, though all their
 * rows have the initial revision
 */
internal fun fillShardTableAtRandom(
    dataSource: DataSource,
    nodes: List<Node>,
    shardsToFill: Iterable<Int> = ShardId.SHARDS_RANGE
) {
    val owners = shardsToFill.associate { shard -> ShardId.of(shard) to nodes.random().id }
    ShardSchema.fillShardTable(dataSource, owners)
}
