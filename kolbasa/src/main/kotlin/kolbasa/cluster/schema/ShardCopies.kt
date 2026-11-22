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
     * For every shard, the newest row of all the copies, see [ShardSchema.mergeShards]
     */
    val mergedShards: Map<ShardId, Shard>,
    /**
     * What every node holds. After [ShardCopies.readOnly], only the nodes that have a shard table
     */
    val copies: SortedMap<Node, Map<ShardId, Shard>>
) {

    /**
     * The shards that have no row on any node
     */
    fun shardsWithoutRow(): List<ShardId> {
        return ShardId.SHARDS_RANGE.map { ShardId.of(it) }.filter { it !in mergedShards }
    }
}

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
        }.toSortedMap()

        // For every shard the newest row of all copies. The database is the only source: our map only saves the reads
        val shardMap = ShardMap(ShardSchema.mergeShards(copies), copies)

        // Shards that no node has: a new cluster, or rows lost everywhere. They get a random node on the node with the
        // smallest id, and the initial revision, so a real row that turns up later wins over them
        val shardsToFill = shardMap.shardsWithoutRow().map { it.id }
        if (shardsToFill.isEmpty()) {
            return shardMap
        }

        val (smallestNode, smallestDataSource) = nodes.entries.first()
        copies[smallestNode] = onShardTable(smallestNode) {
            ShardSchema.fillShardTable(smallestDataSource, nodes.keys.toList(), shardsToFill)
            // Another client may have filled them at the same moment: its rows are the ones in the table now
            ShardSchema.readShards(smallestDataSource)
        }

        return ShardMap(ShardSchema.mergeShards(copies), copies)
    }

    /**
     * Reads the copies on all [nodes] and merges them, and changes nothing: no table is created, and a shard that no node
     * has is missing from the merged map. An error names the node it came from
     */
    fun readOnly(nodes: SortedMap<Node, DataSource>): ShardMap {
        val copies = sortedMapOf<Node, Map<ShardId, Shard>>()

        for ((node, dataSource) in nodes) {
            onShardTable(node) {
                if (ShardSchema.shardTableExists(dataSource)) {
                    copies[node] = ShardSchema.readShards(dataSource)
                }
            }
        }

        return ShardMap(ShardSchema.mergeShards(copies), copies)
    }

    /**
     * The shard numbers that don't exist, for every copy of [shardMap]. Reading the map skips such rows
     */
    fun readInvalidShards(nodes: SortedMap<Node, DataSource>, shardMap: ShardMap): Map<Node, List<Int>> {
        return shardMap.copies.keys.associateWith { node ->
            onShardTable(node) {
                ShardSchema.readInvalidShards(nodes.getValue(node))
            }
        }
    }

    /**
     * Writes to every node the rows of the merged map that its copy lacks, or holds an older or another version of.
     * If copies hold different rows with the same revision, all of them get the row of the merged map.
     *
     * A node that can't take them doesn't stop the others. The first failure is thrown at the end, the others are
     * added to it as suppressed. An error names the node it came from
     */
    fun repair(nodes: SortedMap<Node, DataSource>, shardMap: ShardMap) {
        val shards = resolveTies(shardMap)

        writeToEveryNode(nodes) { node ->
            val copy = shardMap.copies.getValue(node)
            shards.filter { shard -> copy[shard.shard] != shard }
        }
    }

    /**
     * Writes [shards] to every node, and nothing else: the other rows of the copies stay as they are.
     *
     * A node that can't take them doesn't stop the others, the same as in [repair]
     */
    fun write(nodes: SortedMap<Node, DataSource>, shards: List<Shard>) {
        writeToEveryNode(nodes) { shards }
    }

    private fun writeToEveryNode(nodes: SortedMap<Node, DataSource>, rowsToWrite: (Node) -> List<Shard>) {
        var failure: Exception? = null

        for ((node, dataSource) in nodes) {
            val rows = rowsToWrite(node)
            if (rows.isEmpty()) {
                continue
            }

            try {
                onShardTable(node) {
                    ShardSchema.writeShards(dataSource, rows)
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

    // The rows of the merged map. A row that another copy holds with the same revision and other nodes gets a new revision:
    // a table takes only a row that is newer than its own, so with the old one the copies would stay different forever
    private fun resolveTies(shardMap: ShardMap): List<Shard> {
        val newRevision = ShardSchema.nextRevision(shardMap.mergedShards.values)
        val shards = mutableListOf<Shard>()

        for (shard in shardMap.mergedShards.values) {
            val tie = shardMap.copies.values.any { copy ->
                val row = copy[shard.shard]
                row != null && row.revision == shard.revision && !row.equalsIgnoreRevision(shard)
            }

            shards += if (tie) {
                Shard(shard.shard, shard.producerNode, shard.consumerNode, shard.nextConsumerNode, newRevision)
            } else {
                shard
            }
        }

        return shards
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
