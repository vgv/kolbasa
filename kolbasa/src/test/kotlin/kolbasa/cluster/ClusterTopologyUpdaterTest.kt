package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

/**
 * [ClusterTopologyUpdater]: the first update in [ClusterTopologyUpdater.start], the scheduled ones after it, and
 * [ClusterTopologyUpdater.close] that stops them.
 */
class ClusterTopologyUpdaterTest : AbstractPostgreSQLTest() {

    private lateinit var executor: CountingExecutor

    @BeforeEach
    fun setUp() {
        // Our own executor, shut down after each test: nothing a test started outlives it
        executor = CountingExecutor(Executors.newSingleThreadScheduledExecutor())
    }

    @AfterEach
    fun tearDown() {
        executor.shutdownNow()
        executor.awaitTermination(10, TimeUnit.SECONDS)
    }

    @Test
    fun testStart_UpdatesAtOnce() {
        val cluster = Cluster(dataSources(), emptyList())
        val updater = ClusterTopologyUpdater(cluster, options())

        updater.start()

        assertTrue(cluster.isInitialized())
    }

    @Test
    fun testStart_PicksUpChangeLater() {
        val dataSources = dataSources()
        val cluster = Cluster(dataSources, emptyList())
        ClusterTopologyUpdater(cluster, options()).start()

        val (shard, newNode) = moveRandomShard(cluster, dataSources)

        // No updateTopology() here: only the scheduled update can bring the change
        waitUntil { cluster.getTopology().shards[shard]?.producerNode == newNode }
    }

    @Test
    fun testStart_SurvivesFailedUpdate() {
        val failingNode = FailingDataSource(dataSourceSecondSchema)
        val dataSources = listOf(dataSource, dataSourceFirstSchema, failingNode)
        val cluster = Cluster(dataSources, emptyList())
        ClusterTopologyUpdater(cluster, options { }).start()

        // The node goes away for a while: the scheduled updates in between fail
        failingNode.failing = true
        waitUntil { failingNode.failedCalls.get() > 0 }
        failingNode.failing = false

        // The node is back, and the updates go on
        val (shard, newNode) = moveRandomShard(cluster, dataSources)
        waitUntil { cluster.getTopology().shards[shard]?.producerNode == newNode }
    }

    @Test
    fun testStart_FailedUpdateGoesToHandler() {
        val failingNode = FailingDataSource(dataSourceSecondSchema)
        val errors = CopyOnWriteArrayList<TopologyUpdateError>()
        val cluster = Cluster(listOf(dataSource, dataSourceFirstSchema, failingNode), emptyList())
        ClusterTopologyUpdater(cluster, options { errors += it }).start()

        failingNode.failing = true
        waitUntil { errors.isNotEmpty() }

        // What the node threw, maybe wrapped on the way
        val causes = generateSequence(errors.first().cause as Throwable) { it.cause }
        assertTrue(causes.any { it.message == "The node is down" }) { "Unexpected error: ${errors.first().cause}" }
    }

    @Test
    fun testStart_Twice() {
        // A long interval: no scheduled update runs, and schedules again, while the test checks the count
        val updater = ClusterTopologyUpdater(Cluster(dataSources(), emptyList()), options(interval = Duration.ofMinutes(1)))
        updater.start()

        // Does nothing: the updates are already scheduled, and there is no second chain of them
        assertDoesNotThrow { updater.start() }
        assertEquals(1, executor.scheduled.get())
    }

    @Test
    fun testStart_AgainAfterFailedFirstUpdate() {
        val failingNode = FailingDataSource(dataSourceSecondSchema)
        val cluster = Cluster(listOf(dataSource, dataSourceFirstSchema, failingNode), emptyList())
        val updater = ClusterTopologyUpdater(cluster, options())

        // The database isn't there at startup yet
        failingNode.failing = true
        assertThrows<Exception> { updater.start() }
        assertEquals(0, executor.scheduled.get())

        // Now it is, and the same call works
        failingNode.failing = false
        assertDoesNotThrow { updater.start() }
        assertTrue(cluster.isInitialized())
    }

    @Test
    fun testStart_AfterClose() {
        val updater = ClusterTopologyUpdater(Cluster(dataSources(), emptyList()), options())
        updater.close()

        assertThrows<IllegalStateException> { updater.start() }
    }

    @Test
    fun testStart_AfterUpdateTopology() {
        val dataSources = dataSources()
        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()

        // The cluster already works, and the updater adds the schedule
        ClusterTopologyUpdater(cluster, options()).start()

        val (shard, newNode) = moveRandomShard(cluster, dataSources)
        waitUntil { cluster.getTopology().shards[shard]?.producerNode == newNode }
    }

    @Test
    fun testClose_StopsUpdates() {
        val node = FailingDataSource(dataSourceSecondSchema)
        val updater = ClusterTopologyUpdater(Cluster(listOf(dataSource, dataSourceFirstSchema, node), emptyList()), options())
        updater.start()

        updater.close()
        // close() has waited for a running update, so from here on nobody touches the node
        val callsAfterClose = node.calls.get()

        // Three intervals: enough for any update still scheduled to run
        Thread.sleep(3 * ClusterTopologyUpdaterOptions.MIN_INTERVAL.toMillis())
        assertEquals(callsAfterClose, node.calls.get())
    }

    @Test
    fun testClose_ClusterKeepsWorking() {
        val cluster = Cluster(dataSources(), emptyList())
        val updater = ClusterTopologyUpdater(cluster, options())
        updater.start()
        val lastTopology = cluster.getTopology()

        updater.close()

        // The roles keep the last topology, and the cluster can still be updated by hand
        assertTrue(cluster.isInitialized())
        assertSame(lastTopology, cluster.getTopology())
        assertDoesNotThrow { cluster.updateTopology() }
    }

    @Test
    fun testClose_Twice() {
        val updater = ClusterTopologyUpdater(Cluster(dataSources(), emptyList()), options())
        updater.start()
        updater.close()

        assertDoesNotThrow { updater.close() }
    }

    @Test
    fun testClose_WaitsForRunningUpdate() {
        val hangingNode = HangingDataSource(dataSourceSecondSchema)
        val cluster = Cluster(listOf(dataSource, dataSourceFirstSchema, hangingNode), emptyList())
        val updater = ClusterTopologyUpdater(cluster, options { })
        updater.start()

        // The next update hangs on the node
        hangingNode.hanging = true
        assertTrue(hangingNode.entered.await(WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))

        // close() waits for it
        val closing = CompletableFuture.runAsync { updater.close() }
        Thread.sleep(200)
        assertFalse(closing.isDone)

        // The update ends, and so does close()
        hangingNode.release.countDown()
        closing.get(WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun dataSources() = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

    // The shortest interval and our own executor, so the test is fast and can stop the updates
    private fun options(
        interval: Duration = ClusterTopologyUpdaterOptions.MIN_INTERVAL,
        errorHandler: TopologyUpdateErrorHandler = TopologyUpdateErrorHandler.SYSTEM_LOG
    ) = ClusterTopologyUpdaterOptions(
        interval = interval,
        executor = executor,
        errorHandler = errorHandler
    )

    // Gives a random shard to another node behind the cluster's back, straight in the shard table
    private fun moveRandomShard(cluster: Cluster, dataSources: List<DataSource>): Pair<ShardId, NodeId> {
        val topology = cluster.getTopology()
        val shard = ShardId.random()
        val oldNode = requireNotNull(topology.shards[shard]).producerNode
        val newNode = (topology.nodes.keys - oldNode).random()

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

    // An executor that counts the updates scheduled on it
    private class CountingExecutor(
        private val delegate: ScheduledExecutorService
    ) : ScheduledExecutorService by delegate {

        val scheduled = AtomicInteger()

        override fun schedule(command: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture<*> {
            scheduled.incrementAndGet()
            return delegate.schedule(command, delay, unit)
        }
    }

    // A node that counts its getConnection() calls and can be switched off: while failing is set, they throw
    private class FailingDataSource(private val delegate: DataSource) : DataSource by delegate {

        @Volatile
        var failing = false

        val calls = AtomicInteger()
        val failedCalls = AtomicInteger()

        override fun getConnection(): Connection {
            calls.incrementAndGet()
            if (failing) {
                failedCalls.incrementAndGet()
                throw SQLException("The node is down")
            }

            return delegate.connection
        }
    }

    // A node that hangs: while hanging is set, getConnection() waits for the release, then throws
    private class HangingDataSource(private val delegate: DataSource) : DataSource by delegate {

        @Volatile
        var hanging = false

        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)

        override fun getConnection(): Connection {
            if (!hanging) {
                return delegate.connection
            }

            entered.countDown()
            release.await()
            throw SQLException("The node is down")
        }
    }

    private companion object {
        val WAIT_TIMEOUT: Duration = Duration.ofSeconds(10)
    }
}
