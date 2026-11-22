package kolbasa.cluster.admin

import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.readInt
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.postgresql.ds.PGSimpleDataSource
import java.sql.Connection
import java.time.Instant
import javax.sql.DataSource

class ClusterAdminTest : AbstractPostgreSQLTest() {

    @Test
    fun testEmptyNodeListIsRejected() {
        assertThrows<IllegalArgumentException> {
            ClusterAdmin(emptyList())
        }
    }

    @Test
    fun testCreatingAdminChangesNothingInTheDatabase() {
        val tablesBefore = publicTables()

        ClusterAdmin(listOf(dataSource))

        // An operator reaches for ClusterAdmin when the cluster is already broken, so it must not create or
        // seed anything on its way in - unlike Cluster, which initializes what it needs when it starts
        assertEquals(tablesBefore, publicTables())
    }

    @Test
    fun testCreatingAdminDoesNotConnect() {
        val unreachable = PGSimpleDataSource().apply {
            serverNames = arrayOf("127.0.0.1")
            portNumbers = intArrayOf(42)
            databaseName = "there-is-no-such-database"
        }

        // Constructing is fine, an unreachable node only becomes a problem when the cluster is asked
        assertDoesNotThrow { ClusterAdmin(listOf(unreachable)) }
    }

    @Test
    fun testChecksWriteNothing_NewCluster() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        // Only the node ids exist, as after the first check-cluster
        val nodes = ClusterHelper.readNodes(dataSources)

        // A write would fail: the transactions of these nodes are read only
        val admin = ClusterAdmin(dataSources.map { ReadOnlyDataSource(it) })
        runEveryCheck(admin, nodes.keys.first().id)

        dataSources.forEach { assertFalse(ShardSchema.shardTableExists(it)) }
    }

    @Test
    fun testChecksWriteNothing_CopiesDiffer() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, second, _) = nodes.values.toList()

        // The first copy has not all the shards, the second one has other rows, and the third node has no table. The trigger
        // function of the first node is of another kolbasa version
        ShardSchema.createOrUpdateShardTable(first)
        ShardSchema.fillShardTable(first, nodes.keys.toList(), shardsToFill = 0..999)
        ShardSchema.createOrUpdateShardTable(second)
        ShardSchema.fillShardTable(second, nodes.keys.toList(), shardsToFill = 0..499)
        first.useStatement { statement ->
            statement.execute("comment on function ${ShardSchema.SET_REVISION_FUNCTION_NAME}() is 'kolbasa-shard-revision:other'")
        }

        val admin = ClusterAdmin(dataSources.map { ReadOnlyDataSource(it) })
        runEveryCheck(admin, nodes.keys.first().id)
    }

    @Test
    fun testCheckShardMap() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (firstNode, secondNode, thirdNode) = nodes.keys.map { it.id }
        val (first, second, _) = nodes.values.toList()
        val admin = ClusterAdmin(dataSources)

        // No map yet
        assertTrue(admin.checkShardMap().isMissing)

        // An application has started: the same map on every node
        Cluster(dataSources, emptyList()).updateTopology()
        assertTrue(admin.checkShardMap().isClean, "${admin.checkShardMap()}")

        // The second copy lacks a shard and has a row of a shard that doesn't exist, the third node has lost its table, and
        // a shard is given by hand to a node that isn't in the cluster. No application has repaired anything yet
        second.useStatement { statement ->
            statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} = 3")
            statement.execute(
                "insert into ${ShardSchema.SHARD_TABLE_NAME}" +
                    "(${ShardSchema.SHARD_COLUMN_NAME}, ${ShardSchema.PRODUCER_NODE_COLUMN_NAME}, " +
                    "${ShardSchema.CONSUMER_NODE_COLUMN_NAME}) values (5000, 'node-x', 'node-x')"
            )
        }
        nodes.values.last().useStatement { it.execute("drop table ${ShardSchema.SHARD_TABLE_NAME}") }
        first.useStatement { statement ->
            statement.execute(
                "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-gone', " +
                    "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-gone' where ${ShardSchema.SHARD_COLUMN_NAME} = 17"
            )
        }

        val result = admin.checkShardMap()

        assertTrue(result.copies.getValue(firstNode).isUpToDate, "$result")
        assertEquals(
            ShardCopyResult(missing = listOf(ShardId.of(3)), outdated = listOf(ShardId.of(17)), conflicting = emptyList()),
            result.copies[secondNode]
        )
        assertEquals(setOf(thirdNode), result.nodesWithoutTable)
        assertTrue(result.shardsWithoutRow.isEmpty())
        assertEquals(mapOf(secondNode to listOf(5000)), result.invalidShards)
        assertEquals(mapOf(NodeId("node-gone") to listOf(ShardId.of(17))), result.unknownOwners)
    }

    @Test
    fun testCreateOrRepairShardMap_NewCluster() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources).keys.map { it.id }
        val admin = ClusterAdmin(dataSources)

        assertEquals(nodes.associateWith { ShardId.SHARD_COUNT }, admin.createOrRepairShardMap())

        assertTrue(admin.checkShardMap().isClean, "${admin.checkShardMap()}")
        assertTrue(ShardSchema.readShards(dataSource).values.all { it.revision == Instant.EPOCH })

        // Nothing is left to do
        assertEquals(nodes.associateWith { 0 }, admin.createOrRepairShardMap())
    }

    @Test
    fun testCreateOrRepairShardMap_CopiesDiffer() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, second, third) = nodes.values.toList()
        val admin = ClusterAdmin(dataSources)
        Cluster(dataSources, emptyList()).updateTopology()

        // The first node has an edit by hand. The second copy has an older row, a row with the same revision and other nodes,
        // and no shard 7. No node has shard 8. The third node has lost its table
        val shard9 = requireNotNull(ShardSchema.readShards(first)[ShardId.of(9)])
        first.useStatement { statement ->
            statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} = 8")
            statement.execute(
                "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-x', " +
                    "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-x' where ${ShardSchema.SHARD_COLUMN_NAME} = 5"
            )
        }
        second.useStatement { statement ->
            statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} in (7, 8, 9)")
        }
        ShardSchema.writeShards(second, listOf(Shard(ShardId.of(9), NodeId("node-y"), NodeId("node-y"), null, Instant.EPOCH)))
        third.useStatement { it.execute("drop table ${ShardSchema.SHARD_TABLE_NAME}") }

        val changedRows = admin.createOrRepairShardMap()

        assertEquals(listOf(2, 4, ShardId.SHARD_COUNT), nodes.keys.map { changedRows.getValue(it.id) })
        val shards = ShardSchema.readShards(first)
        dataSources.forEach { assertEquals(shards, ShardSchema.readShards(it)) }
        assertEquals(ShardId.SHARD_COUNT, shards.size)
        assertEquals(NodeId("node-x"), shards[ShardId.of(5)]?.producerNode)
        // The row of the node with the smallest id won, with a new revision
        assertTrue(shards.getValue(ShardId.of(9)).equalsIgnoreRevision(shard9))
        assertTrue(shards.getValue(ShardId.of(9)).revision > Instant.EPOCH)
    }

    @Test
    fun testCreateOrRepairShardMap_NodeRejectsRows_OtherNodesRepaired() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (_, middle, last) = nodes.values.toList()
        val middleNode = nodes.keys.toList()[1].id
        val admin = ClusterAdmin(dataSources)
        Cluster(dataSources, emptyList()).updateTopology()

        listOf(middle, last).forEach { node ->
            node.useStatement { statement ->
                statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} < 10")
            }
        }
        middle.useStatement { statement ->
            statement.execute(
                "create function reject_rows() returns trigger language plpgsql as " +
                    "'begin raise exception ''The node takes no rows''; end'"
            )
            statement.execute(
                "create trigger reject_rows before insert or update on ${ShardSchema.SHARD_TABLE_NAME} " +
                    "for each row execute procedure reject_rows()"
            )
        }

        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            admin.createOrRepairShardMap()
        }

        assertTrue("on node ${middleNode.id}" in exception.message, exception.message)
        assertEquals(ShardId.SHARD_COUNT, ShardSchema.readShards(last).size)
    }

    @Test
    fun testPlanNodesDrainProposesEverythingTheNodeHolds() {
        val dataSources = seededCluster()
        val admin = ClusterAdmin(dataSources)
        val drainedNode = ClusterHelper.readNodes(dataSources).keys.first().id

        val plan = admin.planNodesDrain(setOf(drainedNode))

        // Everything the node holds is proposed for moving, and nothing else is
        val held = plan.currentDistribution.getValue(drainedNode)
        assertEquals(held.size, plan.totalMoves)
        assertEquals(held.toSet(), plan.proposedMoves.values.flatten().toSet())
        assertEquals(-held.size, plan.netChangeByNode.getValue(drainedNode))
        // The nodes that stay are the ones taking the shards over
        assertEquals(plan.currentDistribution.keys - drainedNode, plan.proposedMoves.keys)
    }

    @Test
    fun testPlanNodesDrainRefusesANodeThatIsNotInTheCluster() {
        val dataSources = seededCluster()
        val knownNode = ClusterHelper.readNodes(dataSources).keys.first().id

        val exception = assertThrows<ClusterOperationException.DrainUnknownNodeException> {
            ClusterAdmin(dataSources).planNodesDrain(setOf(NodeId("there-is-no-such-node")))
        }

        // The message names the ids that would have worked: an internal node id is easy to mistype
        assertEquals(true, exception.message.contains("there-is-no-such-node"), exception.message)
        assertEquals(true, exception.message.contains(knownNode.id), exception.message)
    }

    @Test
    fun testPlanNodesDrainRefusesTheOnlyNodeOfACluster() {
        val dataSources = listOf(dataSource)
        val theOnlyNode = ClusterHelper.readNodes(dataSources).keys.first().id
        seedShardTable(dataSources)

        // Emptying it would leave its shards nowhere to go
        assertThrows<ClusterOperationException.DrainLastNodeException> {
            ClusterAdmin(dataSources).planNodesDrain(setOf(theOnlyNode))
        }
    }

    @Test
    fun testDrainingNoNodesIsRejected() {
        val admin = ClusterAdmin(seededCluster())

        // An empty set would make the plan a plain rebalance of the whole cluster
        assertThrows<IllegalArgumentException> {
            admin.planNodesDrain(emptySet())
        }
        assertThrows<IllegalArgumentException> {
            admin.drainNodes(emptySet(), shardsPerRound = 200)
        }
    }

    @Test
    fun testRebalanceEvensTheClusterOut() {
        // fillShardTable hands every shard to a random node, so the cluster starts out uneven
        val dataSources = seededCluster()

        ClusterAdmin(dataSources).rebalance(shardsPerRound = 400)

        // 1024 shards over three nodes
        assertEquals(listOf(342, 341, 341), shardsPerNode().values.sortedDescending())
    }

    @Test
    fun testDrainNodesLeavesItOwningNothing() {
        val dataSources = seededCluster()
        val drainedNode = ClusterHelper.readNodes(dataSources).keys.first().id

        ClusterAdmin(dataSources).drainNodes(setOf(drainedNode), shardsPerRound = 200)

        // The shards it held are spread over the two nodes that stay
        val shardsPerNode = shardsPerNode()
        assertEquals(null, shardsPerNode[drainedNode], "$shardsPerNode")
        assertEquals(listOf(512, 512), shardsPerNode.values.sortedDescending())
    }

    @Test
    fun testDrainNodesEmptiesSeveralNodesAtOnce() {
        val dataSources = seededCluster()
        val (stayingNode, drainedNodes) = ClusterHelper.readNodes(dataSources).keys.map { it.id }
            .let { it.first() to it.drop(1).toSet() }

        ClusterAdmin(dataSources).drainNodes(drainedNodes, shardsPerRound = 200)

        // Two of the three nodes drained, so the one that stays holds everything
        assertEquals(mapOf(stayingNode to 1024), shardsPerNode())
    }

    /**
     * Node -> how many shards it owns, as the shard table has it now
     */
    private fun shardsPerNode(): Map<NodeId, Int> =
        ShardSchema.readShards(dataSource).values.groupingBy { it.producerNode }.eachCount()

    /**
     * A three-node cluster – three schemas of one database – with its shard table filled
     */
    private fun seededCluster(): List<DataSource> {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        seedShardTable(dataSources)
        return dataSources
    }

    private fun seedShardTable(dataSources: List<DataSource>) {
        val nodes = ClusterHelper.readNodes(dataSources)
        ShardSchema.createOrUpdateShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList())
    }

    private fun runEveryCheck(admin: ClusterAdmin, node: NodeId) {
        admin.checkShardBalance()
        admin.checkSchemaConsistency()
        admin.findOrphanTables()
        admin.checkMigrationState()
        admin.checkShardMap()
        admin.planNodesDrain(setOf(node))
    }

    // Every transaction of this node is read only
    private class ReadOnlyDataSource(private val delegate: DataSource) : DataSource by delegate {
        override fun getConnection(): Connection {
            return delegate.connection.also { it.isReadOnly = true }
        }
    }

    private fun publicTables(): Int {
        return dataSource.readInt("select count(*) from information_schema.tables where table_schema='public'")
    }

}
