package kolbasa.cluster.butcher.check

import kolbasa.butcher.MigrationStateResult
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.butcher.MoveHelpers
import javax.sql.DataSource

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
            .groupBy({ it.producerNode }, { it.shard })
        return MigrationStateResult(grouped)
    }

    companion object {
        fun check(dataSources: List<DataSource>): MigrationStateResult {
            val nodes = ClusterHelper.readNodes(dataSources)
            val (_, shards) = MoveHelpers.readShards(nodes)
            return MigrationState(shards.values).compute()
        }
    }
}
