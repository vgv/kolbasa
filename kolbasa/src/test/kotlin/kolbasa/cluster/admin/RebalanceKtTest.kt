package kolbasa.cluster.admin

import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.producer.SendRequest
import kolbasa.producer.ShardId
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaHelpers
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import javax.sql.DataSource

/**
 * The rebalance loop against a three-node cluster – three schemas of one database
 *
 * Every case runs the real thing: the plan comes from the shard table and every round is the three
 * migration steps. What is asserted is the loop itself – that it converges, that it stops, that it keeps to
 * the size of a round and that it refuses to start on top of a migration. How the plan is computed is
 * `ShardBalanceTest`'s business.
 */
internal class RebalanceKtTest : AbstractPostgreSQLTest() {

    @Test
    fun testRebalanceSpreadsOneNodeAcrossTheCluster() {
        val dataSources = clusterDataSources()
        seedShardTable(listOf(nodeOf(dataSource)))
        val progress = RecordingProgress()

        rebalance(dataSources, drainedNodes = emptySet(), shardsPerRound = 400, progressCallback = progress)

        // 1024 shards over 3 nodes is 342/341/341, so 682 of them had to leave the node that held them all
        assertEquals(682, progress.shardsMoved)
        assertEquals(progress.rounds.size, progress.finishedRounds)
        assertShardsPerNode(listOf(342, 341, 341))
        assertNothingInFlight()
    }

    @Test
    fun testRebalanceStopsOnceTheClusterIsBalanced() {
        val dataSources = clusterDataSources()
        seedShardTable(listOf(nodeOf(dataSource)))
        rebalance(dataSources, drainedNodes = emptySet(), shardsPerRound = 1024, progressCallback = ProgressCallback.Ignore)

        val progress = RecordingProgress()
        rebalance(dataSources, drainedNodes = emptySet(), shardsPerRound = 1024, progressCallback = progress)

        // A balanced cluster is not a reason to move anything, so the second run does not open a round
        assertEquals(0, progress.finishedRounds)
        assertEquals(0, progress.shardsMoved)
        assertTrue(progress.rounds.isEmpty(), "${progress.rounds}")
    }

    @Test
    fun testRebalanceMovesNoMoreThanOneRoundAtATime() {
        val dataSources = clusterDataSources()
        seedShardTable(listOf(nodeOf(dataSource)))
        val progress = RecordingProgress()

        rebalance(dataSources, drainedNodes = emptySet(), shardsPerRound = 100, progressCallback = progress)

        // 682 shards in rounds of 100: seven rounds, the last one short
        assertEquals(7, progress.finishedRounds)
        progress.rounds.forEach { moves ->
            val shardsInRound = moves.values.sumOf { it.size }
            assertTrue(shardsInRound in 1..100, "round moved $shardsInRound shard(s): $moves")
        }
        assertEquals(682, progress.rounds.sumOf { moves -> moves.values.sumOf { it.size } })
        // The plan shrinks every round, which is what keeps the loop finite
        assertEquals(progress.shardsLeftToMove.sortedDescending(), progress.shardsLeftToMove)
        assertShardsPerNode(listOf(342, 341, 341))
    }

    @Test
    fun testRebalanceCarriesTheRowsOverToTheNewOwner() {
        val dataSources = clusterDataSources()
        dataSources.forEach { SchemaHelpers.createOrUpdateQueues(it, QUEUE) }
        seedShardTable(listOf(nodeOf(dataSource)))
        // Everything is produced to the one node that owns every shard
        val rowsPerShard = 10
        val producer = DatabaseProducer(dataSources.first())
        SHARDS_WITH_DATA.forEach { shard ->
            val request = SendRequest<Int>(
                (1..rowsPerShard).map { SendMessage(it) },
                SendOptions(shard = shard)
            )
            request.effectiveShard = ShardId.of(shard)
            assertEquals(0, producer.send(QUEUE, request).failedMessages)
        }

        rebalance(dataSources, drainedNodes = emptySet(), shardsPerRound = 400, progressCallback = ProgressCallback.Ignore)

        // Every row sits on the node that owns its shard now, and nowhere else
        val owners = ShardSchema.readShards(dataSources.first())
        val dataSourcesByNode = ClusterHelper.readNodes(dataSources).entries.associate { it.key.id to it.value }
        SHARDS_WITH_DATA.forEach { shard ->
            val owner = owners.getValue(ShardId.of(shard)).consumerNode
            dataSourcesByNode.forEach { (node, nodeDataSource) ->
                val expectedRows = if (node == owner) rowsPerShard else 0
                assertEquals(expectedRows, rowsInShard(nodeDataSource, shard), "shard $shard on $node")
            }
        }
    }

    @Test
    fun testDrainEmptiesTheNodeAndLeavesTheClusterWorking() {
        val dataSources = clusterDataSources()
        val nodes = ClusterHelper.readNodes(dataSources).keys.toList()
        seedShardTable(nodes)
        val drainedNode = nodes.first().id
        val progress = RecordingProgress()

        rebalance(dataSources, drainedNodes = setOf(drainedNode), shardsPerRound = 100, progressCallback = progress)

        val shards = ShardSchema.readShards(dataSources.first()).values
        assertEquals(0, shards.count { it.producerNode == drainedNode })
        assertEquals(0, shards.count { it.consumerNode == drainedNode })
        // Every shard of the cluster is still owned by somebody, and none of them is in flight
        assertEquals(ShardId.SHARD_COUNT, shards.count { it.consumerNode != null })
        assertNothingInFlight()
        assertTrue(progress.shardsMoved > 0, "nothing moved at all")
    }

    @Test
    fun testRebalanceRefusesToRunOnTopOfAMigration() {
        val dataSources = clusterDataSources()
        seedShardTable(listOf(nodeOf(dataSource)))
        // Somebody else's migration, prepared and not finished
        val otherNode = nodeOf(dataSourceFirstSchema).id
        prepare(dataSources, otherNode, listOf(ShardId.of(7)), ProgressCallback.Ignore)

        val exception = assertThrows<ClusterOperationException.MigrationAlreadyInFlightException> {
            rebalance(dataSources, drainedNodes = emptySet(), shardsPerRound = 10, progressCallback = ProgressCallback.Ignore)
        }

        assertEquals(mapOf(otherNode to listOf(ShardId.of(7))), exception.migratingShardsByTarget)
        assertTrue(exception.message.contains(otherNode.id), exception.message)
        // Refused means refused: the shard it was asked about is still the only one migrating
        assertEquals(1, ShardSchema.readShards(dataSources.first()).values.count { it.consumerNode == null })
    }

    @Test
    fun testRebalanceRejectsARoundThatMovesNothing() {
        val dataSources = clusterDataSources()

        assertThrows<IllegalArgumentException> {
            rebalance(dataSources, drainedNodes = emptySet(), shardsPerRound = 0, progressCallback = ProgressCallback.Ignore)
        }
    }

    // ---------- helpers ----------

    private fun clusterDataSources(): List<DataSource> =
        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

    /**
     * The node behind [dataSource]
     *
     * Node ids are random strings and [ClusterHelper.readNodes] sorts by them, so the order of that map
     * says nothing about the order of the data sources. A test that puts the shards and the rows on the
     * same node has to ask for it by its data source.
     */
    private fun nodeOf(dataSource: DataSource): Node =
        ClusterHelper.readNodes(clusterDataSources()).entries.first { it.value == dataSource }.key

    /** Hands all 1024 shards to [nodes], which is one node when a skewed cluster is what the test needs */
    private fun seedShardTable(nodes: List<Node>) {
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes)
    }

    private fun assertShardsPerNode(expected: List<Int>) {
        val perNode = ShardSchema.readShards(dataSource).values
            .groupingBy(Shard::producerNode)
            .eachCount()

        assertEquals(expected.sortedDescending(), perNode.values.sortedDescending(), "$perNode")
    }

    private fun assertNothingInFlight() {
        val shards = ShardSchema.readShards(dataSource).values
        assertEquals(0, shards.count { it.consumerNode == null || it.nextConsumerNode != null })
    }

    private fun rowsInShard(dataSource: DataSource, shard: Int): Int =
        dataSource.readInt("select count(*) from ${QUEUE.dbTableName} where shard=$shard")

    /** Reports every round, so a test can see how the loop walked through the plan */
    private class RecordingProgress : ProgressCallback {
        val rounds = mutableListOf<Map<NodeId, List<ShardId>>>()
        val shardsLeftToMove = mutableListOf<Int>()
        var finishedRounds = -1
        var shardsMoved = -1

        override fun rebalanceRoundStart(round: Int, moves: Map<NodeId, List<ShardId>>, shardsLeftToMove: Int) {
            assertEquals(rounds.size + 1, round, "rounds are counted from one, without gaps")
            rounds += moves
            this.shardsLeftToMove += shardsLeftToMove
        }

        override fun rebalanceFinished(rounds: Int, shardsMoved: Int) {
            finishedRounds = rounds
            this.shardsMoved = shardsMoved
        }
    }

    private companion object {
        val QUEUE = Queue.of("rebalance_test", PredefinedDataTypes.Int)
        val SHARDS_WITH_DATA = listOf(0, 1, 500, 1023)
    }
}
