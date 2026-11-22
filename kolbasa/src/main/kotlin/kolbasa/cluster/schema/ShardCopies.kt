package kolbasa.cluster.schema

import kolbasa.cluster.Shard
import kolbasa.producer.ShardId
import kolbasa.schema.Node
import java.util.*
import javax.sql.DataSource

/**
 * The shard map of a cluster, and what the shard table of every node holds
 */
internal class ShardMap(
    /**
     * For every shard, the newest row of all the copies
     */
    val mergedShards: Map<ShardId, Shard>,
    /**
     * What every node holds
     */
    val copies: Map<Node, Map<ShardId, Shard>>
)

/**
 * The copies of the shard map: every node of a cluster keeps one in its `q__shard` table, and for every shard the newest
 * row of all the copies is the right one. Applications and butcher read and repair the copies the same way
 */
internal object ShardCopies {

    /**
     * Reads the copies on all [nodes] and merges them, see [ShardSchema.mergeShards].
     *
     * A node without a shard table gets one. A shard that no node has gets a random node on the node with the smallest
     * id, with the initial revision, see [ShardSchema.INITIAL_REVISION]. An error names the node it came from.
     *
     * [knownShards] is a map the caller already has, and [knownFingerprint] its md5: a node with the same md5 is not
     * read. Pass an empty map and `null` if the caller has none.
     */
    fun read(
        nodes: SortedMap<Node, DataSource>,
        knownShards: Map<ShardId, Shard>,
        knownFingerprint: String?
    ): ShardMap {
        // What every node holds. A node with our md5 holds exactly our map, the others are read
        val copies = nodes.mapValues { (node, dataSource) ->
            onShardTable(node) {
                // A new node gets an empty table here
                ShardSchema.createOrUpdateShardTable(dataSource)

                if (ShardSchema.readFingerprint(dataSource) == knownFingerprint) {
                    knownShards
                } else {
                    ShardSchema.readShards(dataSource)
                }
            }
        }

        // For every shard the newest row of all copies. The database is the only source: our map only saves the reads
        val mergedShards = ShardSchema.mergeShards(copies.values)

        // Shards that no node has: a new cluster, or rows lost everywhere. They get a random node on the node with the
        // smallest id, and the initial revision, so a real row that turns up later wins over them
        val missingShards = ShardId.SHARDS_RANGE.filter { shard -> ShardId.of(shard) !in mergedShards }
        if (missingShards.isEmpty()) {
            return ShardMap(mergedShards, copies)
        }

        val (smallestNode, smallestDataSource) = nodes.entries.first()
        val smallestCopy = onShardTable(smallestNode) {
            ShardSchema.fillShardTable(smallestDataSource, nodes.keys.toList(), missingShards)
            // Another client may have filled them at the same moment: its rows are the ones in the table now
            ShardSchema.readShards(smallestDataSource)
        }

        return ShardMap(ShardSchema.mergeShards(listOf(mergedShards, smallestCopy)), copies + (smallestNode to smallestCopy))
    }

    /**
     * Writes to every node the rows of the merged map that its copy lacks, or holds an older or another version of.
     *
     * A node that can't take them doesn't stop the others. The first failure is thrown at the end, the others are
     * added to it as suppressed. An error names the node it came from
     */
    fun repair(nodes: SortedMap<Node, DataSource>, shardMap: ShardMap) {
        var failure: Exception? = null

        for ((node, dataSource) in nodes) {
            val copy = shardMap.copies.getValue(node)
            val rowsToWrite = shardMap.mergedShards.values.filter { shard -> copy[shard.shard] != shard }
            if (rowsToWrite.isEmpty()) {
                continue
            }

            try {
                onShardTable(node) {
                    ShardSchema.writeShards(dataSource, rowsToWrite)
                }
            } catch (e: Exception) {
                if (failure == null) {
                    failure = e
                } else {
                    failure.addSuppressed(e)
                }
            }
        }

        if (failure != null) {
            throw failure
        }
    }

    // Names the node in any error of its shard table
    private fun <T> onShardTable(node: Node, block: () -> T): T {
        try {
            return block()
        } catch (e: Exception) {
            throw IllegalStateException("Can't read or update ${ShardSchema.SHARD_TABLE_NAME} on node ${node.id.id}", e)
        }
    }
}
