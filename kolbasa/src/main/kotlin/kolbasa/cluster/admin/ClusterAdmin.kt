package kolbasa.cluster.admin

import kolbasa.cluster.admin.check.MigrationState
import kolbasa.cluster.admin.check.OrphanTables
import kolbasa.cluster.admin.check.SchemaConsistency
import kolbasa.cluster.admin.check.ShardBalance
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * Inspects and rebalances a kolbasa cluster
 *
 * Every diagnostic and every step of a shard migration is a method here, so a migration can be driven from
 * code instead of from the command line: look at [checkShardBalance], decide what to move, then
 * [prepareMigration], [moveData] and [finalizeMigration]. The `butcher` CLI is a frontend over this class
 * and uses nothing it does not offer.
 *
 * Creating a ClusterAdmin touches no database: it only remembers the nodes. That matters because an operator
 * reaches for it exactly when a cluster is in a bad state, and a tool that repaired the schema or seeded the
 * shard table on its way in would destroy the evidence it was asked to look at. Compare with
 * [Cluster][kolbasa.cluster.Cluster], which does initialize what it needs when it starts, and is therefore
 * the wrong thing to diagnose a cluster with.
 *
 * Nothing here holds a connection open, so one instance can be kept for as long as its [nodes] are valid,
 * and all of its methods may be called from several threads at once.
 *
 * @param nodes one [DataSource] per cluster node, the same list a [Cluster][kolbasa.cluster.Cluster] would
 * be given
 */
class ClusterAdmin(private val nodes: List<DataSource>) {

    init {
        require(nodes.isNotEmpty()) {
            "Cluster node list must not be empty"
        }
    }

    /**
     * Checks that every node can be reached, and fails describing the ones that cannot
     *
     * The other methods assume the cluster is reachable and report a connection failure as it comes; call
     * this first to get one clear error instead.
     */
    fun checkNodesAvailable() {
        MoveHelpers.checkClusterNodes(nodes)
    }

    /** Reports how the shards are spread across the nodes, and a minimum-movement rebalance if they are uneven */
    fun checkShardBalance(): ShardBalanceResult = ShardBalance.check(nodes)

    /** Reports queue tables whose structure differs between nodes, or that are missing on some of them */
    fun checkSchemaConsistency(): SchemaConsistencyResult = SchemaConsistency.check(nodes)

    /** Reports DLQ and archive tables left behind on a node whose main queue table is gone */
    fun findOrphanTables(): OrphanTablesResult = OrphanTables.check(nodes)

    /** Reports shards that are still mid-migration, which is what an interrupted move leaves behind */
    fun checkMigrationState(): MigrationStateResult = MigrationState.check(nodes)

    /**
     * Marks [shards] as migrating to [target], the first of the three migration steps
     *
     * After this the shards are still consumed from where their data is, but new messages are produced to
     * [target]. No data is moved yet – that is [moveData].
     */
    @JvmOverloads
    fun prepareMigration(
        target: NodeId,
        shards: List<ShardId>,
        progress: ProgressCallback = ProgressCallback.Ignore
    ) {
        prepare(nodes, target, shards, progress)
    }

    /**
     * Copies the rows of every migrating shard to the node that is taking it over, the second migration step
     *
     * Safe to re-run after an interruption: rows already copied are skipped rather than duplicated.
     */
    @JvmOverloads
    fun moveData(
        includeTables: Set<String>? = null,
        excludeTables: Set<String>? = null,
        progress: ProgressCallback = ProgressCallback.Ignore
    ) {
        move(nodes, includeTables, excludeTables, progress)
    }

    /**
     * Hands the migrating shards over to their new node, the last migration step
     *
     * Only call it once [moveData] has nothing left to copy: it stops consuming the shards from their old
     * node, and any rows still there would stay unread.
     */
    @JvmOverloads
    fun finalizeMigration(progress: ProgressCallback = ProgressCallback.Ignore) {
        finalize(nodes, progress)
    }
}
