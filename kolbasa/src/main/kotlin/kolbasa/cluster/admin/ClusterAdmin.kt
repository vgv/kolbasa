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
 * [rebalance] and [drainNodes] are those same three steps in a loop, a batch of shards at a time. Which
 * shards move where comes from [checkShardBalance] and [planNodesDrain], which anyone can read first.
 *
 * Creating a ClusterAdmin touches no database: it only remembers the nodes. That matters because an operator
 * reaches for it exactly when a cluster is in a bad state, and a tool that repaired the schema or seeded the
 * shard table on its way in would destroy the evidence it was asked to look at. Compare with
 * [Cluster][kolbasa.cluster.Cluster], which does initialize what it needs when it starts, and is therefore
 * the wrong thing to diagnose a cluster with.
 *
 * Nor does it give a node an id, except in [initNodes]. Every node must already have the id it is listed under in
 * [nodes], and every other method checks that before anything else. A node without an id fails with
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
     * Reports which nodes [initNodes] would give an id, and with which bucket. Changes nothing
     *
     * Unlike every other method here, it works on nodes without an id: they are what it reports.
     *
     * A new node that holds messages is not refused. It is in [InitNodesPlan.unsafeNodes] when the cluster has other
     * nodes as well.
     *
     * @throws ClusterOperationException.NodeIdMismatchException if a node has another id, or a `q__node` table without
     * its row
     */
    fun planInitNodes(): InitNodesPlan = planInitNodes(nodes)

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
    fun initNodes(): List<NodeId> = initNodes(nodes)

    /**
     * Reports how the shards are spread across the nodes, and a minimum-movement rebalance if they are uneven
     */
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

    /**
     * Reports queue tables whose structure differs between nodes, or that are missing on some of them
     */
    fun checkSchemaConsistency(): SchemaConsistencyResult = SchemaConsistency.check(nodes)

    /**
     * Reports DLQ and archive tables left behind on a node whose main queue table is gone
     */
    fun checkOrphanTables(): OrphanTablesResult = OrphanTables.check(nodes)

    /**
     * Reports shards that are still mid-migration, which is what an interrupted move leaves behind
     */
    fun checkMigrationState(): MigrationStateResult = MigrationState.check(nodes)

    /**
     * Reports how the copies of the shard map on the nodes differ, and the rows that can't be right
     *
     * Changes nothing, and works on a cluster that has no shard map yet.
     */
    fun checkShardMap(): ShardMapResult = ShardMapState.check(nodes)

    /**
     * Reports which node [initShards] would give every shard that has no row on any node. Changes nothing
     *
     * A node that holds messages is not refused. It is in [InitShardsPlan.unsafeNodes] when the cluster gets its first
     * shard map and has other nodes as well.
     */
    fun planInitShards(): InitShardsPlan = planInitShards(nodes)

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
     * @return the node this call gave every shard. Empty when every shard has a row already
     */
    fun initShards(): Map<ShardId, NodeId> = initShards(nodes)

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
    fun syncShards(): Map<NodeId, Int> = syncShards(nodes)

    /**
     * Marks [shards] as migrating to [target], the first of the three migration steps
     *
     * After this the shards are still consumed from where their data is, but new messages are produced to
     * [target]. No data is moved yet – that is [moveData].
     *
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
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
     *
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
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
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
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
        rebalance(nodes, drainedNodes, shardsPerRound = shardsPerRound, progressCallback = progress)
    }

    /**
     * Hands the migrating shards over to their new node, the last migration step
     *
     * Only call it once [moveData] has nothing left to copy: it stops consuming the shards from their old
     * node, and any rows still there would stay unread.
     *
     * @throws ClusterOperationException.IncompleteShardMapException if the shard map is not complete, see
     * [initShards] and [syncShards]
     */
    @JvmOverloads
    fun finalizeMigration(progress: ProgressCallback = ProgressCallback.Ignore) {
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
