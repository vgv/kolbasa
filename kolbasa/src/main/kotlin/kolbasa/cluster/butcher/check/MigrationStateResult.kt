package kolbasa.cluster.butcher.check

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

/**
 * Shards that are in the middle of a migration, grouped by the node taking them over
 *
 * A healthy cluster has none. A shard listed here is already produced to its new node but consumed by
 * nobody, which is the state `prepare` leaves it in until `finalize` completes the handover – so a
 * non-empty report means either a migration is running right now, or one was interrupted and never finished.
 */
data class MigrationStateResult(
    val migratingShardsByTarget: Map<NodeId, List<ShardId>>,
) {

    /** How many shards are in flight across the whole cluster */
    val totalMigratingShards: Int = migratingShardsByTarget.values.sumOf { it.size }

    /** True when no shard is in flight */
    val isClean: Boolean = totalMigratingShards == 0
}
