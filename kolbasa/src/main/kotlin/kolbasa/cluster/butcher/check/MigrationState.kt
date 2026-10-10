package kolbasa.cluster.butcher.check

import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.butcher.MoveHelpers
import kolbasa.cluster.butcher.config.Command

/**
 * Report shards that are currently in migration state (`consumerNode == null`,
 * `nextConsumerNode != null`). Operators use this to see which shards are in
 * flight between `prepare-migration` and `finalize-migration`.
 */
internal class MigrationState(private val shards: Collection<Shard>) {

    fun compute(): MigrationStateResult {
        // In migration state, the Shard invariant guarantees
        // producerNode == nextConsumerNode (see Shard.init), so we can key by producerNode.
        val grouped = shards
            .filter { it.consumerNode == null }
            .groupBy { it.producerNode }
        return MigrationStateResult(grouped)
    }

    companion object {
        fun check(command: Command.Check): MigrationStateResult {
            val nodes = ClusterHelper.readNodes(command.nodes.dataSources)
            val (_, shards) = MoveHelpers.readShards(nodes)
            return MigrationState(shards.values).compute()
        }
    }
}
