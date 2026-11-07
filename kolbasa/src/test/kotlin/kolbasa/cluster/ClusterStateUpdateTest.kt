package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.schema.NodeId
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Isolated
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

/**
 * The scheduled state update started by [Cluster.initAndScheduleStateUpdate].
 *
 * Isolated: the interval and the executor come from the global [Kolbasa.clusterStateUpdateConfig].
 */
@Isolated
class ClusterStateUpdateTest : AbstractPostgreSQLTest() {

    private lateinit var savedConfig: ClusterStateUpdateConfig
    private lateinit var executor: ScheduledExecutorService

    @BeforeEach
    fun setUp() {
        savedConfig = Kolbasa.clusterStateUpdateConfig
        // Our own executor: Cluster can't stop its update chain, so the test stops it by shutting the executor down
        executor = Executors.newSingleThreadScheduledExecutor()
        Kolbasa.clusterStateUpdateConfig = ClusterStateUpdateConfig(ClusterStateUpdateConfig.MIN_INTERVAL, executor)
    }

    @AfterEach
    fun tearDown() {
        Kolbasa.clusterStateUpdateConfig = savedConfig
        executor.shutdownNow()
        executor.awaitTermination(10, TimeUnit.SECONDS)
    }

    @Test
    fun testInitAndSchedule_UpdatesAtOnce() {
        val cluster = Cluster(dataSources())

        cluster.initAndScheduleStateUpdate()

        assertTrue(cluster.clusterStateUpdatedAtLeastOnce())
    }

    @Test
    fun testInitAndSchedule_PicksUpChangeLater() {
        val dataSources = dataSources()
        val cluster = Cluster(dataSources)
        cluster.initAndScheduleStateUpdate()

        val (shard, newNode) = moveRandomShard(cluster, dataSources)

        // No updateStateOnce() here: only the scheduled update can bring the change
        waitUntil { cluster.getState().shards[shard]?.producerNode == newNode }
    }

    private fun dataSources() = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

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

    private companion object {
        val WAIT_TIMEOUT: Duration = Duration.ofSeconds(10)
    }
}
