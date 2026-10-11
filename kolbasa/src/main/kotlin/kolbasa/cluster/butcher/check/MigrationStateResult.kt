package kolbasa.cluster.butcher.check

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId

internal data class MigrationStateResult(
    val migratingShardsByTarget: Map<NodeId, List<ShardId>>,
) {

    val totalMigratingShards: Int = migratingShardsByTarget.values.sumOf { it.size }

    val isClean: Boolean = totalMigratingShards == 0
}
