package kolbasa.cluster.butcher

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.schema.NodeId

internal fun check(command: Command.Check) {
    MoveHelpers.checkClusterNodes(command.nodes)

    command.checks.forEach { check ->
        when (check) {
            AvailableCheck.CHECK_SHARD_BALANCE -> checkShardBalance(command)
            AvailableCheck.CHECK_SCHEMA_CONSISTENCY -> checkSchemaConsistency(command)
            AvailableCheck.FIND_ORPHAN_TABLES -> findOrphanTables(command)
            AvailableCheck.CHECK_MIGRATION_STATE -> checkMigrationState(command)
        }
    }
}

private fun checkShardBalance(command: Command.Check) {
    val nodes = ClusterHelper.readNodes(command.nodes.dataSources)
    val (_, shards) = MoveHelpers.readShards(nodes)

    val r = mutableMapOf<NodeId, MutableList<Shard>>()
    shards.forEach { (_, shard) ->
        r.computeIfAbsent(shard.producerNode) { mutableListOf() }.add(shard)
    }





    println("TODO: check shard distribution")
}

private fun checkSchemaConsistency(command: Command.Check) {
    println("TODO: check schema consistency")
}

private fun findOrphanTables(command: Command.Check) {
    println("TODO: find orphan tables")
}

private fun checkMigrationState(command: Command.Check) {
    val nodes = ClusterHelper.readNodes(command.nodes.dataSources)
    val (_, shards) = MoveHelpers.readShards(nodes)

    shards.values.forEach { shard ->
        shard.producerNode
    }

}
