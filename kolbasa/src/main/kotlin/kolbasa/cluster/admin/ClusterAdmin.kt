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
 * [rebalance] and [drainNodes] are those same three steps in a loop, a batch of shards at a time. Which
 * shards move where comes from [checkShardBalance] and [planNodesDrain], which anyone can read first.
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

    /**
     * Reports what the cluster would look like with [drainedNodes] holding nothing, and the moves that get
     * it there
     *
     * Reads like [checkShardBalance] and changes nothing. [ShardBalanceResult.currentDistribution] shows
     * [drainedNodes] with everything they hold now, [ShardBalanceResult.proposedMoves] names the nodes that
     * would take those shards over.
     *
     * Only the shards of [drainedNodes] are proposed, spread over the remaining nodes as evenly as they go.
     * The nodes that stay are left alone, so the cluster can be uneven afterwards.
     *
     * [drainedNodes] must not be empty.
     *
     * @throws ClusterOperationException.DrainUnknownNodeException if a node in [drainedNodes] is not a member
     * of this cluster
     * @throws ClusterOperationException.DrainLastNodeException if [drainedNodes] are all the nodes of this
     * cluster
     */
    fun planNodesDrain(drainedNodes: Set<NodeId>): ShardBalanceResult {
        requireNodesToDrain(drainedNodes)
        return ShardBalance.check(nodes, drainedNodes)
    }

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
     * Evens the shards out across the cluster, [shardsPerRound] of them at a time
     *
     * [checkShardBalance]'s plan, carried out instead of printed. While the plan has anything in it, this
     * prepares a batch of at most [shardsPerRound] shards, moves their rows, finalizes them and reads the
     * plan again. Every round is [prepareMigration], [moveData] and [finalizeMigration] in that order, and
     * [progress] reports each one.
     *
     * [shardsPerRound] is a decision about pause, not about speed. A shard between prepare and finalize is
     * consumed by no node, so its messages wait for the rows to arrive. A round of ten leaves 1% of the
     * queue unread at a time, a round of a thousand leaves all of it. On the other hand, every round costs
     * its own prepare and finalize.
     *
     * An interrupted run leaves the finished rounds done and the running one mid-migration.
     * [checkMigrationState] reports it, [moveData] and [finalizeMigration] complete it. This also refuses
     * to start while any shard is migrating: it finalizes every migrating shard, not only its own, and it
     * will not adopt a migration it did not start.
     *
     * @throws ClusterOperationException.MigrationAlreadyInFlightException if a shard is already migrating
     */
    @JvmOverloads
    fun rebalance(shardsPerRound: Int, progress: ProgressCallback = ProgressCallback.Ignore) {
        rebalance(nodes, drainedNodes = emptySet(), shardsPerRound = shardsPerRound, progressCallback = progress)
    }

    /**
     * Moves everything [drainedNodes] own onto the other nodes, [shardsPerRound] shards at a time
     *
     * What [planNodesDrain] proposes, carried out the way [rebalance] carries out a balance plan. Pause,
     * interruption and a migration already in flight all work the same way there.
     *
     * It ends when [drainedNodes] own nothing: nothing is produced to them and nothing is consumed from
     * them. Only their shards are moved, so the cluster can be left uneven. [rebalance] is what evens it out.
     *
     * [drainedNodes] must not be empty.
     *
     * @throws ClusterOperationException.DrainUnknownNodeException if a node in [drainedNodes] is not a member
     * of this cluster
     * @throws ClusterOperationException.DrainLastNodeException if [drainedNodes] are all the nodes of this
     * cluster
     * @throws ClusterOperationException.MigrationAlreadyInFlightException if a shard is already migrating
     */
    @JvmOverloads
    fun drainNodes(
        drainedNodes: Set<NodeId>,
        shardsPerRound: Int,
        progress: ProgressCallback = ProgressCallback.Ignore
    ) {
        requireNodesToDrain(drainedNodes)
        rebalance(nodes, drainedNodes, shardsPerRound = shardsPerRound, progressCallback = progress)
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

    /** With no nodes to drain the plan would be a plain rebalance, which is not what the caller asked for */
    private fun requireNodesToDrain(drainedNodes: Set<NodeId>) {
        require(drainedNodes.isNotEmpty()) {
            "Nodes to drain must not be empty"
        }
    }
}
