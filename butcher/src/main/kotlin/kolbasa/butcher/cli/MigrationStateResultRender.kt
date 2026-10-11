package kolbasa.butcher.cli

import kolbasa.butcher.MigrationStateResult

internal fun MigrationStateResult.render(): String = buildString {
    if (isClean) {
        append("Migration state: no shards in migration")
        return@buildString
    }

    appendLine("Migration state: $totalMigratingShards shard(s) in migration")
    migratingShardsByTarget.toSortedMap().forEach { (target, shards) ->
        val sorted = shards.sorted()
        appendLine("  ⟶ ${target.id} (${sorted.size} shards):")
        appendLine("    shards: ${sorted.joinToString(separator = ",") { it.id.toString() }}")
        appendLine("    target: ${target.id}")
    }
}.trimEnd()
