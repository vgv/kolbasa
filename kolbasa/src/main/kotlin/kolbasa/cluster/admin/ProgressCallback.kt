package kolbasa.cluster.admin

import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import javax.sql.DataSource

/**
 * Reports what a migration is doing while it does it
 *
 * A migration of a large queue can run for a long time, so the three `move` methods are called as the work
 * goes: once per table and node pair at the start and the end, and once per batch of rows in between. The
 * other two are called once, when a step has finished and the shard table has been updated.
 *
 * Every method does nothing by default, so an implementation only overrides what it wants to hear about, and
 * code that wants to hear nothing passes [Ignore].
 *
 * Calls arrive on the thread that is doing the migration, and a move of several tables reports them one
 * after another, so an implementation should be quick and must not throw.
 */
interface ProgressCallback {

    /** The shard table now marks [shards] as migrating to [targetNode]; no data has moved yet */
    fun prepareSuccessful(shards: List<ShardId>, targetNode: NodeId, shardsDiff: List<ShardDiff>) {}

    /** Copying of one table from one node to another is about to start */
    fun moveStart(tableName: String, from: DataSource, to: DataSource) {}

    /**
     * One batch of rows has been copied
     *
     * @param batchRowsMoved how many rows this batch carried. Never zero: a batch that finds nothing left to
     * copy ends the table instead of reporting itself
     * @param tableRowsMoved how many rows have been copied from this table so far, including this batch
     * @param totalRowsMoved how many rows have been copied from all tables so far, including this batch
     */
    fun moveNextBatch(
        tableName: String,
        from: DataSource,
        to: DataSource,
        batchRowsMoved: Int,
        tableRowsMoved: Long,
        totalRowsMoved: Long
    ) {
    }

    /** Copying of one table from one node to another has finished, having moved [tableRowsMoved] rows in total */
    fun moveEnd(tableName: String, from: DataSource, to: DataSource, tableRowsMoved: Long) {}


    /** The migrating shards now belong to their new node, and the migration is over */
    fun finalizeSuccessful(shardsDiff: List<ShardDiff>) {}

    companion object {
        /** A callback that reports nothing */
        @JvmField
        val Ignore: ProgressCallback = object : ProgressCallback {}
    }
}

/**
 * How one shard's ownership changed during a migration step
 *
 * Only the shards that actually changed are reported, so a step that changed nothing produces an empty list.
 */
data class ShardDiff(
    val originalShard: ShardOwnership,
    val updatedShard: ShardOwnership
)
