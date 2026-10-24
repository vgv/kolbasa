package kolbasa.butcher.command

import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.testing.test
import com.zaxxer.hikari.HikariDataSource
import kolbasa.butcher.ButcherCli
import kolbasa.cluster.Cluster
import kolbasa.cluster.admin.ClusterAdmin
import kolbasa.producer.ShardId
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import kolbasa.test.AbstractPostgreSQLTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import javax.sql.DataSource

/**
 * The two high-level commands as an operator meets them: a real cluster, a real config file, the real
 * command line
 *
 * Every other test in this module stops before the cluster is touched, because everything else butcher
 * does is parsing and rendering. A high-level command does its whole job past that point, so the path from
 * the command line to a changed shard table is worth running once end to end.
 *
 * Three nodes, as three schemas of one database: a kolbasa node is its own `q__node` identity, not its own server
 */
class HighLevelCommandsTest : AbstractPostgreSQLTest() {

    @TempDir
    lateinit var tempDir: File

    private val dataSources: List<DataSource>
        get() = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

    @BeforeEach
    fun before() {
        // Seeded the way a real cluster is: by an application starting against it. That creates every
        // node's identity, the shard table with all 1024 shards, and the queue on every node
        Cluster(dataSources, listOf(QUEUE)).updateStateOnce()
    }

    @Test
    fun testRebalanceClusterEvensOutARealCluster() {
        // Everything on one node, the state a freshly added node leaves behind
        val crowdedNode = nodeIds().first()
        giveEveryShardTo(crowdedNode)

        val result = cli().test("rebalance-cluster --shards-per-round=200 --yes ${config().absolutePath}")

        assertEquals(0, result.statusCode, result.output)
        // 1024 shards over 3 nodes leaves 682 to move: four rounds of at most 200
        assertTrue(result.output.contains("Round 1: 200 to "), result.output)
        assertTrue(result.output.contains("Moved 682 shard(s) in 4 round(s)"), result.output)
        assertTrue(result.output.contains("Completed successfully"), result.output)
        // The last word is the distribution it ended up with, read back from the cluster
        assertTrue(result.output.contains("evenly spread across 3 node(s)"), result.output)
        assertTrue(ClusterAdmin(dataSources).checkShardBalance().isBalanced, result.output)
    }

    @Test
    fun testRebalanceClusterOnABalancedClusterDoesNothing() {
        giveEveryShardTo(nodeIds().first())
        cli().test("rebalance-cluster --shards-per-round=1024 --yes ${config().absolutePath}")

        val result = cli().test("rebalance-cluster --shards-per-round=200 --yes ${config().absolutePath}")

        assertEquals(0, result.statusCode, result.output)
        // Not a single round: there was nothing to agree to and nothing to do
        assertTrue(!result.output.contains("Round 1"), result.output)
        assertTrue(result.output.contains("evenly spread across 3 node(s)"), result.output)
    }

    @Test
    fun testDrainNodesEmptiesTheNodeItIsPointedAt() {
        val drainedNode = nodeIds().first()

        val result = cli().test(
            "drain-nodes --nodes=${drainedNode.id} --shards-per-round=300 --yes ${config().absolutePath}"
        )

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("Draining ${drainedNode.id}"), result.output)
        assertTrue(result.output.contains("Node ${drainedNode.id} owns no shards"), result.output)
        // Not only in the report: the shard table agrees
        val distribution = ClusterAdmin(dataSources).checkShardBalance().currentDistribution
        assertEquals(0, distribution.getValue(drainedNode).size, "$distribution")
        assertEquals(1024, distribution.values.sumOf { it.size }, "$distribution")
    }

    @Test
    fun testDrainNodesEmptiesSeveralNodesAtOnce() {
        val (stayingNode, drainedNodes) = nodeIds().let { it.first() to it.drop(1) }

        val result = cli().test(
            "drain-nodes --nodes=${drainedNodes.joinToString(",") { it.id }} --shards-per-round=300 --yes " +
                config().absolutePath
        )

        assertEquals(0, result.statusCode, result.output)
        val names = drainedNodes.joinToString(", ") { it.id }
        assertTrue(result.output.contains("Nodes $names own no shards"), result.output)
        // Two of the three nodes drained, so the one that stays holds everything
        val distribution = ClusterAdmin(dataSources).checkShardBalance().currentDistribution
        assertEquals(1024, distribution.getValue(stayingNode).size, "$distribution")
    }

    @Test
    fun testDrainNodesRefusesANodeTheClusterDoesNotHave() {
        val result = cli().test("drain-nodes --nodes=node-404 --shards-per-round=10 --yes ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("node-404"), result.output)
        // The ids that would have worked: an internal id is easy to get wrong
        nodeIds().forEach { assertTrue(result.output.contains(it.id), result.output) }
    }

    @Test
    fun testCommandRefusesAConfigThatNamesNodesWrongly() {
        // Names that are not the nodes' own ids, the way every config file had them before
        val config = config(publicId = "line-1", firstId = "line-2", secondId = "line-3")

        val result = cli().test("rebalance-cluster --shards-per-round=10 --yes ${config.absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("Node ids don't match"), result.output)
        // The real ids are in the message, ready to copy into the file
        dataSources.forEach { assertTrue(result.output.contains(nodeIdOf(it)), result.output) }
    }

    @Test
    fun testCheckClusterInitializesANewNode() {
        // A node just added to the cluster: an empty schema nothing has started against yet
        dataSources.first().connection.use { connection ->
            connection.createStatement().use { statement -> statement.execute("create schema node_four") }
        }
        val config = config().apply { appendText(configLine("node-four", "node_four")) }

        val result = cli().test("check-cluster ${config.absolutePath}")

        assertEquals(0, result.statusCode, result.output)
        assertTrue(result.output.contains("Initialized node node-four"), result.output)
        assertEquals("node-four", dataSourceFor("node_four").use { nodeIdOf(it) })
    }

    @Test
    fun testHighLevelCommandRefusesToRunOnTopOfAMigration() {
        // Skewed on purpose: with nothing to move, a rebalance stops before it ever looks for a
        // migration in flight, and a seeded cluster is only unbalanced by chance
        giveEveryShardTo(nodeIds().first())
        val target = nodeWithout(shard = 5)
        // A hand-driven migration, prepared and deliberately not finished
        val prepared = cli().test(
            "prepare-migration --target=${target.id} --shards=5 --yes ${config().absolutePath}"
        )
        assertEquals(0, prepared.statusCode, prepared.output)

        val result = cli().test("rebalance-cluster --shards-per-round=10 --yes ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("already migrating"), result.output)
        assertTrue(result.output.contains(target.id), result.output)
        // And it really did refuse: shard 5 is still the one and only shard in flight
        assertEquals(1, ClusterAdmin(dataSources).checkMigrationState().totalMigratingShards, result.output)
    }

    @Test
    fun testRebalanceRefusesAMigrationEvenWithNothingToMove() {
        // An even cluster has no plan at all, so the refusal has to come before the plan is looked at.
        // Shards no node consumes are the one thing worth hearing about here
        giveEveryShardTo(nodeIds().first())
        cli().test("rebalance-cluster --shards-per-round=1024 --yes ${config().absolutePath}")

        // One shard from the heaviest node to the lightest keeps every node at floor or ceil, so the
        // balance report still has nothing to propose
        val distribution = ClusterAdmin(dataSources).checkShardBalance().currentDistribution
        prepareOneShard(
            from = distribution.maxBy { (_, shards) -> shards.size }.key,
            to = distribution.minBy { (_, shards) -> shards.size }.key
        )
        assertTrue(ClusterAdmin(dataSources).checkShardBalance().isBalanced, "the cluster must still be even")

        val result = cli().test("rebalance-cluster --shards-per-round=10 --yes ${config().absolutePath}")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("already migrating"), result.output)
    }

    @Test
    fun testDrainRefusesAMigrationEvenWhenTheNodeIsAlreadyEmpty() {
        val drainedNode = nodeIds().first()
        val drainArguments = "--nodes=${drainedNode.id} --shards-per-round=300 --yes ${config().absolutePath}"
        cli().test("drain-nodes $drainArguments")
        // Somebody else's migration, between the two nodes that stay. The drained node keeps owning
        // nothing, so this drain still has no plan of its own
        val remainingNodes = nodeIds().filterNot { it == drainedNode }
        prepareOneShard(from = remainingNodes.first(), to = remainingNodes.last())

        val result = cli().test("drain-nodes $drainArguments")

        assertEquals(1, result.statusCode, result.output)
        assertTrue(result.output.contains("already migrating"), result.output)
    }

    // ---------- helpers ----------

    /** Prepares one shard of [from] for [to], a migration these tests only need to leave in flight */
    private fun prepareOneShard(from: NodeId, to: NodeId) {
        val shard = ClusterAdmin(dataSources).checkShardBalance().currentDistribution.getValue(from).first()

        val prepared = cli().test(
            "prepare-migration --target=${to.id} --shards=${shard.id} --yes ${config().absolutePath}"
        )
        assertEquals(0, prepared.statusCode, prepared.output)
    }

    /**
     * A node that does not own [shard], so preparing [shard] for it is a real migration
     *
     * A freshly seeded cluster spreads its shards at random, so who holds a given one is not known in
     * advance. Preparing a shard for the node that already owns it is refused, and rightly so.
     */
    private fun nodeWithout(shard: Int): NodeId {
        val distribution = ClusterAdmin(dataSources).checkShardBalance().currentDistribution
        val owner = distribution.entries.first { (_, shards) -> ShardId.of(shard) in shards }.key

        return distribution.keys.sorted().first { it != owner }
    }

    private fun nodeIds(): List<NodeId> =
        ClusterAdmin(dataSources).checkShardBalance().currentDistribution.keys.sorted()

    /**
     * Hands every shard to one node, with a plain `update` against the shard table
     *
     * Seeding spreads the shards at random, which is only slightly uneven. A test that counts rounds needs
     * to know exactly how much there was to move. Writing `q__shard` by hand is ordinary operator work,
     * see docs/Architecture.md.
     */
    private fun giveEveryShardTo(node: NodeId) {
        shardTableHolder().connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    "update q__shard set producer_node='${node.id}', consumer_node='${node.id}', " +
                        "next_consumer_node=null"
                )
            }
        }
    }

    /**
     * The node the shard table lives on
     *
     * There is one shard table per cluster and the library picks which node keeps it, so a test has to
     * look for it. `to_regclass` resolves through the connection's search path, which here is one node's
     * own schema.
     */
    private fun shardTableHolder(): DataSource = dataSources.first { dataSource ->
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("select to_regclass('q__shard') is not null").use { resultSet ->
                    resultSet.next() && resultSet.getBoolean(1)
                }
            }
        }
    }

    private fun dataSourceFor(schema: String): HikariDataSource = HikariDataSource().apply {
        jdbcUrl = pgContainer.jdbcUrl
        username = pgContainer.username
        password = pgContainer.password
        this.schema = schema
    }

    /**
     * The cluster as butcher reads it: one line per node, pointing at one schema each
     *
     * Each line is named by the node's own id, unless a test passes a wrong one.
     */
    private fun config(
        publicId: String = nodeIdOf(dataSource),
        firstId: String = nodeIdOf(dataSourceFirstSchema),
        secondId: String = nodeIdOf(dataSourceSecondSchema),
    ): File {
        val file = File(tempDir, "cluster.conf")
        file.writeText(
            configLine(publicId, "public") +
                configLine(firstId, FIRST_SCHEMA_NAME) +
                configLine(secondId, SECOND_SCHEMA_NAME)
        )
        return file
    }

    /** One node of the cluster, as a line of the config file */
    private fun configLine(id: String, schema: String): String =
        "$id  host=${pgContainer.host} port=${pgContainer.firstMappedPort} " +
            "user=${pgContainer.username} password=${pgContainer.password} " +
            "dbname=${pgContainer.databaseName} schema=$schema\n"

    /** The id the application gave this node when it started */
    private fun nodeIdOf(dataSource: DataSource): String =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("select id from q__node").use { resultSet ->
                    resultSet.next()
                    resultSet.getString(1)
                }
            }
        }

    private fun cli() = ButcherCli().subcommands(
        CheckCluster(),
        RebalanceCluster(),
        DrainNodes(),
        PrepareMigration(),
        MoveData(),
        FinalizeMigration(),
    )

    private companion object {
        val QUEUE: Queue<Int> = Queue.of("high_level_test", PredefinedDataTypes.Int)
    }
}
