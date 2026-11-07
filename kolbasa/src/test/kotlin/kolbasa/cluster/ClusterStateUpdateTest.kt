package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

/**
 * The scheduled state update started by [Cluster.initAndScheduleStateUpdate].
 */
class ClusterStateUpdateTest : AbstractPostgreSQLTest() {

    private lateinit var executor: ScheduledExecutorService

    @BeforeEach
    fun setUp() {
        // Our own executor: Cluster can't stop its update chain, so the test stops it by shutting the executor down
        executor = Executors.newSingleThreadScheduledExecutor()
    }

    @AfterEach
    fun tearDown() {
        executor.shutdownNow()
        executor.awaitTermination(10, TimeUnit.SECONDS)
    }

    @Test
    fun testInitAndSchedule_UpdatesAtOnce() {
        val cluster = Cluster(dataSources(), options = options())

        cluster.initAndScheduleStateUpdate()

        assertTrue(cluster.clusterStateUpdatedAtLeastOnce())
    }

    @Test
    fun testInitAndSchedule_PicksUpChangeLater() {
        val dataSources = dataSources()
        val cluster = Cluster(dataSources, options = options())
        cluster.initAndScheduleStateUpdate()

        val (shard, newNode) = moveRandomShard(cluster, dataSources)

        // No updateStateOnce() here: only the scheduled update can bring the change
        waitUntil { cluster.getState().shards[shard]?.producerNode == newNode }
    }

    @Test
    fun testInitAndSchedule_SurvivesFailedUpdate() {
        val failingNode = FailingDataSource(dataSourceSecondSchema)
        val dataSources = listOf(dataSource, dataSourceFirstSchema, failingNode)
        val cluster = Cluster(dataSources, options = options { })
        cluster.initAndScheduleStateUpdate()

        // The node goes away for a while: the scheduled updates in between fail
        failingNode.failing = true
        waitUntil { failingNode.failedCalls.get() > 0 }
        failingNode.failing = false

        // The node is back, and the updates go on
        val (shard, newNode) = moveRandomShard(cluster, dataSources)
        waitUntil { cluster.getState().shards[shard]?.producerNode == newNode }
    }

    @Test
    fun testInitAndSchedule_FailedUpdateGoesToHandler() {
        val failingNode = FailingDataSource(dataSourceSecondSchema)
        val errors = CopyOnWriteArrayList<StateUpdateError>()
        val cluster = Cluster(listOf(dataSource, dataSourceFirstSchema, failingNode), options = options { errors += it })
        cluster.initAndScheduleStateUpdate()

        failingNode.failing = true
        waitUntil { errors.isNotEmpty() }

        // What the node threw, maybe wrapped on the way
        val causes = generateSequence(errors.first().cause as Throwable) { it.cause }
        assertTrue(causes.any { it.message == "The node is down" }) { "Unexpected error: ${errors.first().cause}" }
    }

    private fun dataSources() = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

    // The shortest interval and our own executor, so the test is fast and can stop the updates
    private fun options(errorHandler: StateUpdateErrorHandler = StateUpdateErrorHandler.SYSTEM_LOG) = ClusterOptions(
        stateUpdateInterval = ClusterOptions.MIN_STATE_UPDATE_INTERVAL,
        stateUpdateExecutor = executor,
        stateUpdateErrorHandler = errorHandler
    )

    // Gives a random shard to another node behind the cluster's back, straight in the shard table
    private fun moveRandomShard(cluster: Cluster, dataSources: List<DataSource>): Pair<ShardId, NodeId> {
        val state = cluster.getState()
        val shard = ShardId.random()
        val oldNode = requireNotNull(state.shards[shard]).producerNode
        val newNode = (state.nodes.keys - oldNode).random()

        // The shard table lives on one node only
        val shardTableNode: DataSource = dataSources.single { dataSource ->
            runCatching { ShardSchema.readShards(dataSource).size == ShardId.SHARD_COUNT }.getOrDefault(false)
        }

        shardTableNode.useStatement { statement ->
            statement.execute(
                """
                update ${ShardSchema.SHARD_TABLE_NAME}
                set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = '${newNode.id}',
                    ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = '${newNode.id}'
                where ${ShardSchema.SHARD_COLUMN_NAME} = ${shard.id}
                """.trimIndent()
            )
        }

        return shard to newNode
    }

    private fun waitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos()
        while (!condition()) {
            check(System.nanoTime() < deadline) { "The condition wasn't met in $WAIT_TIMEOUT" }
            Thread.sleep(50)
        }
    }

    // A node that can be switched off: while failing is set, getConnection() throws
    private class FailingDataSource(private val delegate: DataSource) : DataSource by delegate {

        @Volatile
        var failing = false

        val failedCalls = AtomicInteger()

        override fun getConnection(): Connection {
            if (failing) {
                failedCalls.incrementAndGet()
                throw SQLException("The node is down")
            }

            return delegate.connection
        }
    }

    private companion object {
        val WAIT_TIMEOUT: Duration = Duration.ofSeconds(10)
    }
}
