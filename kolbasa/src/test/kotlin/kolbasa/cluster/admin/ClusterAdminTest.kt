package kolbasa.cluster.admin

import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.Shard
import kolbasa.cluster.fillShardTableAtRandom
import kolbasa.cluster.fillShardTableEvenly
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.IdSchema
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
            ClusterAdmin(emptyMap())
        }
    }

    @Test
    fun testCreatingAdminChangesNothingInTheDatabase() {
        val tablesBefore = publicTables()

        ClusterAdmin(mapOf(NodeId("node-01") to dataSource))

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
        assertDoesNotThrow { ClusterAdmin(mapOf(NodeId("node-01") to unreachable)) }
    }

    @Test
    fun testChecksWriteNothing_NewCluster() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        // Only the node ids exist, as after the first check-cluster
        val nodes = ClusterHelper.readNodes(dataSources)

        // A write would fail: the transactions of these nodes are read only
        val admin = ClusterAdmin(nodesById(dataSources).mapValues { (_, node) -> ReadOnlyDataSource(node) })
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
        fillShardTableEvenly(first, nodes.keys.toList(), shardsToFill = 0..999)
        ShardSchema.createOrUpdateShardTable(second)
        fillShardTableAtRandom(second, nodes.keys.toList(), shardsToFill = 0..499)
        first.useStatement { statement ->
            statement.execute("comment on function ${ShardSchema.SET_REVISION_FUNCTION_NAME}() is 'kolbasa-shard-revision:other'")
        }

        val admin = ClusterAdmin(nodesById(dataSources).mapValues { (_, node) -> ReadOnlyDataSource(node) })
        runEveryCheck(admin, nodes.keys.first().id)
    }

    @Test
    fun testNodeWithoutIdIsRefused() {
        // Two nodes have their ids, the third one has never been set up
        val nodes = nodesById(listOf(dataSource, dataSourceFirstSchema)) + (NEW_NODE to dataSourceSecondSchema)

        // A write would fail: the transactions of these nodes are read only
        val admin = ClusterAdmin(nodes.mapValues { (_, node) -> ReadOnlyDataSource(node) })
        everyMethod(admin, nodes.keys.first()).forEach { method ->
            val exception = assertThrows<ClusterOperationException.NodesNotInitializedException> { method() }
            assertEquals(listOf(NEW_NODE), exception.nodes)
        }

        // planInitNodes is the one check that works on such a node, and it writes nothing either
        assertEquals(listOf(NEW_NODE), admin.planInitNodes().newNodes.map { node -> node.id })

        // The node is not given an id, and no shard map is created anywhere
        assertFalse(IdSchema.nodeTableExists(dataSourceSecondSchema))
        nodes.values.forEach { assertFalse(ShardSchema.shardTableExists(it)) }
    }

    @Test
    fun testNodeWithAnotherIdIsRefused() {
        val setUpNodes = nodesById(listOf(dataSource, dataSourceFirstSchema))
        val (renamedNode, renamed) = setUpNodes.entries.first()
        // One node is listed under a wrong id, and the third one has never been set up
        val nodes = setUpNodes - renamedNode + (WRONG_ID to renamed) + (NEW_NODE to dataSourceSecondSchema)

        // The wrong id wins: with a wrong node list it doesn't matter which nodes have no id yet
        val admin = ClusterAdmin(nodes.mapValues { (_, node) -> ReadOnlyDataSource(node) })
        (everyMethod(admin, nodes.keys.first()) + initMethods(admin)).forEach { method ->
            val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> { method() }
            assertEquals(mapOf(WRONG_ID to renamedNode), exception.mismatches)
            assertEquals(emptyList<NodeId>(), exception.nodesWithEmptyTable)
        }
    }

    @Test
    fun testNodeTableWithoutRowIsRefused() {
        val nodes = nodesById(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        val (emptyNode, emptyTable) = nodes.entries.last()
        emptyTable.useStatement { it.execute("delete from ${IdSchema.NODE_TABLE_NAME}") }

        val admin = ClusterAdmin(nodes.mapValues { (_, node) -> ReadOnlyDataSource(node) })
        (everyMethod(admin, nodes.keys.first()) + initMethods(admin)).forEach { method ->
            val exception = assertThrows<ClusterOperationException.NodeIdMismatchException> { method() }
            assertEquals(emptyMap<NodeId, NodeId>(), exception.mismatches)
            assertEquals(listOf(emptyNode), exception.nodesWithEmptyTable)
        }
    }

    @Test
    fun testCheckShardMap() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (firstNode, secondNode, thirdNode) = nodes.keys.map { it.id }
        val (first, second, _) = nodes.values.toList()
        val admin = ClusterAdmin(nodesById(dataSources))

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
        val admin = ClusterAdmin(nodesById(dataSources))

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
        val admin = ClusterAdmin(nodesById(dataSources))
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
        val admin = ClusterAdmin(nodesById(dataSources))
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
    fun testOperationsNeedACompleteShardMap_NewCluster() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources).keys.map { it.id }

        assertEveryOperationRefuses(dataSources, nodes.toSet(), ShardId.SHARDS_RANGE.map { ShardId.of(it) }.toSet())
    }

    @Test
    fun testOperationsNeedACompleteShardMap_NodeWithoutTable() {
        val dataSources = seededCluster()
        val (lastNode, last) = ClusterHelper.readNodes(dataSources).entries.last()
        last.useStatement { it.execute("drop table ${ShardSchema.SHARD_TABLE_NAME}") }

        assertEveryOperationRefuses(dataSources, setOf(lastNode.id), emptySet())
    }

    @Test
    fun testOperationsNeedACompleteShardMap_ShardsNoNodeHas() {
        val dataSources = seededCluster()
        dataSources.forEach { node ->
            node.useStatement { statement ->
                statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} in (5, 6)")
            }
        }

        assertEveryOperationRefuses(dataSources, emptySet(), setOf(ShardId.of(5), ShardId.of(6)))
    }

    @Test
    fun testPlanNodesDrainProposesEverythingTheNodeHolds() {
        val dataSources = seededCluster()
        val admin = ClusterAdmin(nodesById(dataSources))
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
            ClusterAdmin(nodesById(dataSources)).planNodesDrain(setOf(NodeId("there-is-no-such-node")))
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
            ClusterAdmin(nodesById(dataSources)).planNodesDrain(setOf(theOnlyNode))
        }
    }

    @Test
    fun testDrainingNoNodesIsRejected() {
        val admin = ClusterAdmin(nodesById(seededCluster()))

        // An empty set would make the plan a plain rebalance of the whole cluster
        assertThrows<IllegalArgumentException> {
            admin.planNodesDrain(emptySet())
        }
        assertThrows<IllegalArgumentException> {
            admin.drainNodes(emptySet(), shardsPerRound = 200)
        }
    }

    @Test
    fun testNewShardMapIsEven() {
        val dataSources = seededCluster()
        val nodes = ClusterHelper.readNodes(dataSources).keys.map { it.id }

        // The nodes take turns in the order of their ids: the first one gets shard 0, and the one shard left over
        assertEquals(mapOf(nodes[0] to 342, nodes[1] to 341, nodes[2] to 341), shardsPerNode())
        assertEquals(nodes[0], ShardSchema.readShards(dataSource).getValue(ShardId.of(0)).producerNode)
        // So a new cluster has nothing to rebalance
        assertTrue(ClusterAdmin(nodesById(dataSources)).checkShardBalance().isBalanced)
    }

    @Test
    fun testRebalanceEvensTheClusterOut() {
        // Every shard on one node
        val dataSources = seededCluster()
        giveEveryShardTo(ClusterHelper.readNodes(dataSources).keys.first().id, dataSources)

        ClusterAdmin(nodesById(dataSources)).rebalance(shardsPerRound = 400)

        // 1024 shards over three nodes
        assertEquals(listOf(342, 341, 341), shardsPerNode().values.sortedDescending())
    }

    @Test
    fun testDrainNodesLeavesItOwningNothing() {
        val dataSources = seededCluster()
        val drainedNode = ClusterHelper.readNodes(dataSources).keys.first().id

        ClusterAdmin(nodesById(dataSources)).drainNodes(setOf(drainedNode), shardsPerRound = 200)

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

        ClusterAdmin(nodesById(dataSources)).drainNodes(drainedNodes, shardsPerRound = 200)

        // Two of the three nodes drained, so the one that stays holds everything
        assertEquals(mapOf(stayingNode to 1024), shardsPerNode())
    }

    /**
     * Hands every shard to [node], on every copy of the shard map
     */
    private fun giveEveryShardTo(node: NodeId, dataSources: List<DataSource>) {
        dataSources.forEach { dataSource ->
            dataSource.useStatement { statement ->
                statement.execute(
                    "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = '${node.id}', " +
                        "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = '${node.id}', " +
                        "${ShardSchema.NEXT_CONSUMER_NODE_COLUMN_NAME} = null"
                )
            }
        }
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
        ClusterAdmin(nodesById(dataSources)).createOrRepairShardMap()
    }

    private fun assertEveryOperationRefuses(
        dataSources: List<DataSource>,
        nodesWithoutTable: Set<NodeId>,
        shardsWithoutRow: Set<ShardId>
    ) {
        val node = ClusterHelper.readNodes(dataSources).keys.first().id
        // The transactions of these nodes are read only, so an operation that writes before it refuses fails otherwise
        val admin = ClusterAdmin(nodesById(dataSources).mapValues { (_, node) -> ReadOnlyDataSource(node) })

        listOf<() -> Unit>(
            { admin.prepareMigration(node, listOf(ShardId.of(1))) },
            { admin.moveData() },
            { admin.finalizeMigration() },
            { admin.rebalance(shardsPerRound = 100) },
            { admin.drainNodes(setOf(node), shardsPerRound = 100) },
        ).forEach { operation ->
            val exception = assertThrows<ClusterOperationException.IncompleteShardMapException> { operation() }
            assertEquals(nodesWithoutTable, exception.nodesWithoutTable)
            assertEquals(shardsWithoutRow, exception.shardsWithoutRow)
        }
    }

    /**
     * Every public method of [admin] that needs every node to have its id, with [node] wherever one needs a node
     */
    private fun everyMethod(admin: ClusterAdmin, node: NodeId): List<() -> Unit> = listOf(
        { admin.checkShardBalance() },
        { admin.planNodesDrain(setOf(node)) },
        { admin.checkSchemaConsistency() },
        { admin.checkOrphanTables() },
        { admin.checkMigrationState() },
        { admin.checkShardMap() },
        { admin.createOrRepairShardMap() },
        { admin.prepareMigration(node, listOf(ShardId.of(1))) },
        { admin.moveData() },
        { admin.rebalance(shardsPerRound = 100) },
        { admin.drainNodes(setOf(node), shardsPerRound = 100) },
        { admin.finalizeMigration() },
    )

    /**
     * The two methods of [admin] that work on nodes without an id. A wrong id stops them as well
     */
    private fun initMethods(admin: ClusterAdmin): List<() -> Unit> = listOf(
        { admin.planInitNodes() },
        { admin.initNodes() },
    )

    private fun runEveryCheck(admin: ClusterAdmin, node: NodeId) {
        admin.checkShardBalance()
        admin.checkSchemaConsistency()
        admin.checkOrphanTables()
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

    private companion object {
        val NEW_NODE = NodeId("node-new")
        val WRONG_ID = NodeId("node-wrong")
    }

}
