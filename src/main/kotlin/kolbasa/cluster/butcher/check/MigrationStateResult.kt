package kolbasa.cluster.butcher.check

import kolbasa.cluster.Shard
import kolbasa.schema.NodeId

internal data class MigrationStateResult(
    val migratingShardsByTarget: Map<NodeId, List<Shard>>,
) {

    val totalMigratingShards: Int = migratingShardsByTarget.values.sumOf { it.size }

    val isClean: Boolean = totalMigratingShards == 0

    override fun toString(): String = buildString {
        if (isClean) {
            append("Migration state: no shards in migration")
            return@buildString
        }

        appendLine("Migration state: $totalMigratingShards shard(s) in migration")
        migratingShardsByTarget.toSortedMap().forEach { (target, shards) ->
            val sorted = shards.sortedBy { it.shard }
            appendLine("  -> ${target.id} (${sorted.size} shards):")
            sorted.forEach { shard ->
                appendLine("    shard ${shard.shard.toString().padStart(4)}")
            }
        }
    }.trimEnd()
}
