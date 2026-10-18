package kolbasa.cluster.admin

import kolbasa.AbstractPostgreSQLTest
import kolbasa.cluster.ClusterHelper
import kolbasa.cluster.schema.ShardSchema
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.postgresql.ds.PGSimpleDataSource
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

    /** Node -> how many shards it owns, as the shard table has it now */
    private fun shardsPerNode(): Map<NodeId, Int> =
        ShardSchema.readShards(dataSource).values.groupingBy { it.producerNode }.eachCount()

    /** A three-node cluster – three schemas of one database – with its shard table filled */
    private fun seededCluster(): List<DataSource> {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        seedShardTable(dataSources)
        return dataSources
    }

    private fun seedShardTable(dataSources: List<DataSource>) {
        val nodes = ClusterHelper.readNodes(dataSources)
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList())
    }

    private fun publicTables(): Int {
        return dataSource.readInt("select count(*) from information_schema.tables where table_schema='public'")
    }

}
