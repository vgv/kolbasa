package kolbasa.cluster.admin

import kolbasa.cluster.admin.check.MigrationState
import kolbasa.cluster.admin.check.OrphanTables
import kolbasa.cluster.admin.check.SchemaConsistency
import kolbasa.cluster.admin.check.ShardBalance
import kolbasa.cluster.admin.check.ShardMapState
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
 * [rebalanceCluster] and [drainNodes] are those same three steps in a loop, a batch of shards at a time. Which
 * shards move where comes from [checkShardBalance] and [planDrainNodes], which anyone can read first.
 *
 * Creating a ClusterAdmin touches no database: it only remembers the nodes. That matters because an operator
 * reaches for it exactly when a cluster is in a bad state, and a tool that repaired the schema or seeded the
 * shard table on its way in would destroy the evidence it was asked to look at. Compare with
 * [Cluster][kolbasa.cluster.Cluster], which does initialize what it needs when it starts, and is therefore
 * the wrong thing to diagnose a cluster with.
 *
 * Every method first checks that every node can be reached, and fails with
 * [ClusterOperationException.NodesUnreachableException] naming the ones that can't.
 *
 * Nor does it give a node an id, except in [initNodes] and [initCluster]. Every node must already have the id it is
 * listed under in [nodes], and every other method checks that before anything else. A node without an id fails with
 * [ClusterOperationException.NodesNotInitializedException], a node with another id with
 * [ClusterOperationException.NodeIdMismatchException].
 *
 * Nothing here holds a connection open, so one instance can be kept for as long as its [nodes] are valid,
 * and all of its methods may be called from several threads at once.
 *
 * @param nodes every cluster node by its id, the one in its `q__node` table. The [DataSource]s are the ones a
 * [Cluster][kolbasa.cluster.Cluster] would be given
 */
class ClusterAdmin(private val nodes: Map<NodeId, DataSource>) {

    init {
        require(nodes.isNotEmpty()) {
            "Cluster node list must not be empty"
        }
    }

    /**
     * Reports what [initCluster] would do: which nodes get an id, which shards get a node, and how many copies of the
     * shard map get rows from the others. Changes nothing
     *
     * Like [planInitNodes], it works on nodes without an id. Their shards are worked out for the ids they are listed
     * under, which are the ids they get.
     *
     * A node that holds messages is not refused. It is in [InitClusterPlan.unsafeNodes] when the cluster has other nodes
     * as well, and the node gets an id or the cluster gets its first shard map.
     *
     * @throws ClusterOperationException.NodeIdMismatchException if a node has another id, or a `q__node` table without
     * its row
     */
    fun planInitCluster(): InitClusterPlan {
        ClusterHelpers.checkReachable(nodes)
        return planInitCluster(nodes)
    }

    /**
     * Sets the cluster up, or brings it up to date: [initNodes], then [initShards], then [syncShards]
     *
     * On a new cluster that gives every node its id and creates the shard map on every node. On a cluster with a new
     * node, it gives that node its id and a copy of the map. On a cluster that is set up already it does nothing, so
     * it is safe to call again. If a step fails, the steps after it don't run: call it again once the cause is fixed.
     *
     * This doesn't look at [planInitCluster], and sets the cluster up on an unsafe plan as well: read the plan first.
     *
     * @throws ClusterOperationException.NodeIdMismatchException if a node has another id, or a `q__node` table without
     * its row. Then nothing is written
     */
    fun initCluster(): InitClusterResult {
        ClusterHelpers.checkReachable(nodes)
        return initCluster(nodes)
    }

    /**
     * Reports which nodes [initNodes] would give an id, and with which bucket. Changes nothing
     *
     * Like [planInitCluster], and unlike the other methods here, it works on nodes without an id: they are what it
     * reports.
     *
     * A new node that holds messages is not refused. It is in [InitNodesPlan.unsafeNodes] when the cluster has other
     * nodes as well.
     *
     * @throws ClusterOperationException.NodeIdMismatchException if a node has another id, or a `q__node` table without
     * its row
     */
    fun planInitNodes(): InitNodesPlan {
        ClusterHelpers.checkReachable(nodes)
        return planInitNodes(nodes)
    }

    /**
     * Gives every node without an id the one it is listed under in [nodes], and a bucket of its own
     *
     * A new node gets the lowest bucket no other node uses, the nodes in the order of their ids. If a node has another
     * id, or a `q__node` table without its row, no node gets an id: a wrong id means the node list is wrong, and a wrong
     * list must not set up anything.
     *
     * This doesn't look at [planInitNodes], and gives an unsafe node its id as well: read the plan first. Call this
     * before a [Cluster][kolbasa.cluster.Cluster] sees the new nodes. A Cluster gives a node without an id a random one.
     *
     * @return the nodes this call gave an id to. A node that another process set up at the same moment is not among them
     * @throws ClusterOperationException.NodeIdMismatchException if a node has another id, or a `q__node` table without
     * its row
     */
    fun initNodes(): List<NodeId> {
        ClusterHelpers.checkReachable(nodes)
        return initNodes(nodes)
    }

    /**
     * Runs [checks], all of them by default, and returns what they found. Changes nothing
     *
     * Every check is a method of its own as well, see [ClusterCheck]. Unlike them, this refuses a cluster that has no
     * shard map on any node: such a cluster is not set up yet, and there is nothing to check.
     *
     * @throws ClusterOperationException.ShardsNotInitializedException if there is no shard map on any node, see
     * [initCluster]
     */
    @JvmOverloads
    fun checkCluster(checks: Set<ClusterCheck> = ClusterCheck.entries.toSet()): CheckResult {
        ClusterHelpers.checkReachable(nodes)
        return checkCluster(nodes, checks)
    }

    /**
     * Reports how the shards are spread across the nodes, and a minimum-movement rebalance if they are uneven
     */
    fun checkShardBalance(): ShardBalanceResult {
        ClusterHelpers.checkReachable(nodes)
        return ShardBalance.check(nodes)
    }

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
    fun planDrainNodes(drainedNodes: Set<NodeId>): ShardBalanceResult {
        requireNodesToDrain(drainedNodes)
        ClusterHelpers.checkReachable(nodes)
        return ShardBalance.check(nodes, drainedNodes)
    }

    /**
     * Reports queue tables whose structure differs between nodes, or that are missing on some of them
     */
    fun checkSchemaConsistency(): SchemaConsistencyResult {
        ClusterHelpers.checkReachable(nodes)
        return SchemaConsistency.check(nodes)
    }

    /**
     * Reports DLQ and archive tables left behind on a node whose main queue table is gone
     */
    fun checkOrphanTables(): OrphanTablesResult {
        ClusterHelpers.checkReachable(nodes)
        return OrphanTables.check(nodes)
    }

    /**
     * Reports shards that are still mid-migration, which is what an interrupted move leaves behind
     */
    fun checkMigrationState(): MigrationStateResult {
        ClusterHelpers.checkReachable(nodes)
        return MigrationState.check(nodes)
    }

    /**
     * Reports how the copies of the shard map on the nodes differ, and the rows that can't be right
     *
     * Changes nothing, and works on a cluster that has no shard map yet.
     */
    fun checkShardMap(): ShardMapResult {
        ClusterHelpers.checkReachable(nodes)
        return ShardMapState.check(nodes)
    }

    /**
     * Reports which node [initShards] would give every shard that has no row on any node. Changes nothing
     *
     * A node that holds messages is not refused. It is in [InitShardsPlan.unsafeNodes] when the cluster gets its first
     * shard map and has other nodes as well.
     */
    fun planInitShards(): InitShardsPlan {
        ClusterHelpers.checkReachable(nodes)
        return planInitShards(nodes, target = null, shards = null)
    }

    /**
     * Reports which shards [initShards] would give [target]: every shard that has no row on any node, or only [shards].
     * Changes nothing
     *
     * It refuses the same way as [initShards] with a target does. A node that holds messages is in
     * [InitShardsPlan.unsafeNodes] when the cluster gets its first shard map, has other nodes as well, and the node is
     * not [target].
     *
     * @throws ClusterOperationException.MoveToNonExistingNodeException if [target] is not a node of this cluster
     * @throws ClusterOperationException.ShardsAlreadyInitializedException if a shard of [shards] has a row on some node
     */
    @JvmOverloads
    fun planInitShards(target: NodeId, shards: List<ShardId>? = null): InitShardsPlan {
        ClusterHelpers.checkReachable(nodes)
        return planInitShards(nodes, target, shards)
    }

    /**
     * Gives a node to every shard that has no row on any node: the node with the fewest shards at that moment, the
     * smallest id of them on a tie. On a cluster without a shard map that is every shard, spread evenly over the nodes,
     * so this is how a new map is created.
     *
     * Every node gets a shard table if it has none, and the new rows. They have the initial revision, so a real row wins
     * over them, even one that turns up later. A shard that has a row on some node stays as it is, and so do copies
     * that differ: that is [syncShards].
     *
     * A node that takes no rows doesn't stop the others. Then this throws, with the name of that node. The new rows are
     * in effect once one node has them, and [syncShards] brings them to the other nodes once they are back.
     *
     * This doesn't look at [planInitShards], and gives the shards out on an unsafe plan as well: read the plan first.
     *
     * @return the node of every shard this call gave one to, read back after the write. Empty when every shard has a
     * row already
     */
    fun initShards(): Map<ShardId, NodeId> {
        ClusterHelpers.checkReachable(nodes)
        return initShards(nodes, target = null, shards = null)
    }

    /**
     * Gives [target] every shard that has no row on any node, or only [shards]
     *
     * The rest is the same as in [initShards] without a target: every node gets the new rows, with the initial revision.
     * A shard that has a row on some node has a node already, and changing it is a migration, see [prepareMigration].
     * So if one of [shards] has a row, no shard gets a node.
     *
     * Running applications give the shards without a row a node on their next topology update, spread evenly over the
     * nodes. So this is for a cluster with no application running.
     *
     * @return the node of every shard this call gave one to, read back after the write. An application may have given a
     * shard a node at the same moment and won: then that shard has another node than [target]
     * @throws ClusterOperationException.MoveToNonExistingNodeException if [target] is not a node of this cluster
     * @throws ClusterOperationException.ShardsAlreadyInitializedException if a shard of [shards] has a row on some node
     */
    @JvmOverloads
    fun initShards(target: NodeId, shards: List<ShardId>? = null): Map<ShardId, NodeId> {
        ClusterHelpers.checkReachable(nodes)
        return initShards(nodes, target, shards)
    }

    /**
     * Brings the copies of the shard map on the nodes up to date, the way an application does on every topology update
     *
     * A node without a shard table gets one, and every copy gets the newest row of every shard. If copies hold different
     * rows with the same revision, the row of the node with the smallest id wins, with a new revision. A shard that no
     * node has gets no node here, see [initShards].
     *
     * A node that takes no rows doesn't stop the others. Then this throws, with the name of that node. Call it again once
     * the node is back: it does only what is still missing.
     *
     * Rows of shards that don't exist, and shards of nodes that are not in the cluster, stay as they are: see
     * [checkShardMap].
     *
     * @return how many rows have changed on every node, zero for a copy that was up to date already
     * @throws ClusterOperationException.ShardsNotInitializedException if there is no shard map on any node
     */
    fun syncShards(): Map<NodeId, Int> {
        ClusterHelpers.checkReachable(nodes)
        return syncShards(nodes)
    }

    /**
     * Marks [shards] as migrating to [target], the first of the three migration steps
     *
     * After this the shards are still consumed from where their data is, but new messages are produced to
     * [target]. No data is moved yet – that is [moveData].
     *
     * @throws ClusterOperationException.ShardsNotInitializedException if there is no shard map on any node, see
     * [initCluster]
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
     */
    @JvmOverloads
    fun prepareMigration(
        target: NodeId,
        shards: List<ShardId>,
        progress: ProgressCallback = ProgressCallback.Ignore
    ) {
        ClusterHelpers.checkReachable(nodes)
        prepare(nodes, target, shards, progress)
    }

    /**
     * Copies the rows of every migrating shard to the node that is taking it over, the second migration step
     *
     * Safe to re-run after an interruption: rows already copied are skipped rather than duplicated.
     *
     * @throws ClusterOperationException.ShardsNotInitializedException if there is no shard map on any node, see
     * [initCluster]
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
     */
    @JvmOverloads
    fun moveData(
        includeTables: Set<String>? = null,
        excludeTables: Set<String>? = null,
        progress: ProgressCallback = ProgressCallback.Ignore
    ) {
        ClusterHelpers.checkReachable(nodes)
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
     * @throws ClusterOperationException.ShardsNotInitializedException if there is no shard map on any node, see
     * [initCluster]
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
     * @throws ClusterOperationException.MigrationAlreadyInFlightException if a shard is already migrating
     */
    @JvmOverloads
    fun rebalanceCluster(shardsPerRound: Int, progress: ProgressCallback = ProgressCallback.Ignore) {
        ClusterHelpers.checkReachable(nodes)
        rebalance(nodes, drainedNodes = emptySet(), shardsPerRound = shardsPerRound, progressCallback = progress)
    }

    /**
     * Moves everything [drainedNodes] own onto the other nodes, [shardsPerRound] shards at a time
     *
     * What [planDrainNodes] proposes, carried out the way [rebalanceCluster] carries out a balance plan. Pause,
     * interruption and a migration already in flight all work the same way there.
     *
     * It ends when [drainedNodes] own nothing: nothing is produced to them and nothing is consumed from
     * them. Only their shards are moved, so the cluster can be left uneven. [rebalanceCluster] is what evens it out.
     *
     * [drainedNodes] must not be empty.
     *
     * @throws ClusterOperationException.DrainUnknownNodeException if a node in [drainedNodes] is not a member
     * of this cluster
     * @throws ClusterOperationException.DrainLastNodeException if [drainedNodes] are all the nodes of this
     * cluster
     * @throws ClusterOperationException.ShardsNotInitializedException if there is no shard map on any node, see
     * [initCluster]
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
     * @throws ClusterOperationException.MigrationAlreadyInFlightException if a shard is already migrating
     */
    @JvmOverloads
    fun drainNodes(
        drainedNodes: Set<NodeId>,
        shardsPerRound: Int,
        progress: ProgressCallback = ProgressCallback.Ignore
    ) {
        requireNodesToDrain(drainedNodes)
        ClusterHelpers.checkReachable(nodes)
        rebalance(nodes, drainedNodes, shardsPerRound = shardsPerRound, progressCallback = progress)
    }

    /**
     * Hands the migrating shards over to their new node, the last migration step
     *
     * Only call it once [moveData] has nothing left to copy: it stops consuming the shards from their old
     * node, and any rows still there would stay unread.
     *
     * @throws ClusterOperationException.ShardsNotInitializedException if there is no shard map on any node, see
     * [initCluster]
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
     */
    @JvmOverloads
    fun finalizeMigration(progress: ProgressCallback = ProgressCallback.Ignore) {
        ClusterHelpers.checkReachable(nodes)
        finalize(nodes, progress)
    }

    /**
     * With no nodes to drain the plan would be a plain rebalance, which is not what the caller asked for
     */
    private fun requireNodesToDrain(drainedNodes: Set<NodeId>) {
        require(drainedNodes.isNotEmpty()) {
            "Nodes to drain must not be empty"
        }
    }
}
