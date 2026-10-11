package kolbasa.cluster.butcher.check

import kolbasa.cluster.Shard
import kolbasa.schema.NodeId

internal data class MigrationStateResult(
    val migratingShardsByTarget: Map<NodeId, List<Shard>>,
) {

    val totalMigratingShards: Int = migratingShardsByTarget.values.sumOf { it.size }

    val isClean: Boolean = totalMigratingShards == 0
}
