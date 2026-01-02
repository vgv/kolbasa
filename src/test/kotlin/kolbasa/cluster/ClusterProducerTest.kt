package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import kolbasa.Kolbasa
import kolbasa.cluster.schema.ShardSchema
import kolbasa.consumer.Message
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.producer.SendRequest
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.IdSchema
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.sql.Statement
import javax.sql.DataSource
import kotlin.collections.isNotEmpty

class ClusterProducerTest : AbstractPostgresqlTest() {

    private val messagesToSend = 1_000

    private val queue = Queue.of("test", PredefinedDataTypes.Int)

    private val dataSources by lazy { listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema) }
    private lateinit var cluster: Cluster
    private lateinit var clusterProducer: ClusterProducer

    @BeforeEach
    fun before() {
        Kolbasa.shardStrategy = ShardStrategy.Random

        dataSources.forEach { dataSource ->
            SchemaHelpers.createOrUpdateQueues(dataSource, queue)
        }

        cluster = Cluster(dataSources)
        cluster.updateStateOnce()

        clusterProducer = ClusterProducer(cluster)
    }

    @Test
    fun testMessagesDistribution_SendOneMessageAndCheckShard() {
        val shard = 123

        val sendRequest = SendRequest(listOf(SendMessage(shard)), SendOptions(shard = shard))
        val sendResult = clusterProducer.send(queue, sendRequest)

        // read directly from the producer node
        val producerNode = requireNotNull(cluster.getState().shards[shard]?.producerNode)
        val dataSource = requireNotNull(cluster.getState().nodes[producerNode])
        val consumer = DatabaseConsumer(dataSource)
        val rawMessages = consumer.receive(queue, messagesToSend)

        assertEquals(1, rawMessages.size)
        assertEquals(sendResult.onlySuccessful().map { it.id }, rawMessages.map { it.id })
        assertEquals(sendResult.onlySuccessful().map { it.message.data }, rawMessages.map { it.data })
    }

    @Test
    fun testMessagesDistribution_SendOneMessageIfProducerNodeFailed() {
        // change producer node to 'unknown' for this dataSource
        val node = requireNotNull(IdSchema.readNodeInfo(dataSource))
        val stringId: String = node.id.id
        findDataSourceWithInitializedShard(dataSources).useStatement { statement: Statement ->
            val sql = """
                       update
                            ${ShardSchema.SHARD_TABLE_NAME}
                       set
                            ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'unknown',
                            ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'unknown'
                       where
                            ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = '$stringId'
                   """.trimIndent()
            statement.executeUpdate(sql)
        }
        // re-read cluster state after shards changing
        cluster.updateStateOnce()

        // send to random nodes
        val results = (1..messagesToSend).map {
            val shard = ShardStrategy.Random.getShard()
            val sendRequest = SendRequest<Int>(listOf(SendMessage(shard)), SendOptions(shard = shard))
            clusterProducer.send(queue, sendRequest)
        }

        // Test that all messages were sent successfully
        results.forEach { sendResult ->
            assertEquals(0, sendResult.failedMessages)
            sendResult.onlySuccessful().forEach {
                assertEquals(it.message.data, it.id.shard)
            }
        }

        // Read from the specified shard using direct consumer
        val first = readData(dataSource)
        val second = readData(dataSourceFirstSchema)
        val third = readData(dataSourceSecondSchema)

        // Check that "non-existing" node didn't receive any messages
        assertEquals(0, first.size)
        // Check that other nodes received all messages
        assertEquals(messagesToSend, second.size + third.size)
    }

    @Test
    fun testMessagesDistribution_SendOneMessageIfAllProducerNodesNotFound() {
        // change producer node to 'unknown' for all dataSource
        findDataSourceWithInitializedShard(dataSources).useStatement { statement: Statement ->
            val sql = """
                       update
                            ${ShardSchema.SHARD_TABLE_NAME}
                       set
                            ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'unknown',
                            ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'unknown'
                   """.trimIndent()
            statement.executeUpdate(sql)
        }
        // re-read cluster state after shards changing
        cluster.updateStateOnce()
        // check that all producer nodes are NOT THE SAME that really existing nodes
        val allProducerNodes = cluster.getState().shards.values.map { it.producerNode }.toSet()
        val allNodes = cluster.getState().nodes.keys
        assertTrue(allNodes.intersect(allProducerNodes).isEmpty(), "allNodes: $allNodes, allProducerNodes: $allProducerNodes")

        // send to random nodes
        val results = (1..messagesToSend).map {
            val shard = ShardStrategy.Random.getShard()
            val sendRequest = SendRequest(listOf(SendMessage(shard)), SendOptions(shard = shard))
            clusterProducer.send(queue, sendRequest)
        }

        // Test that all messages were sent successfully
        results.forEach { sendResult ->
            assertEquals(0, sendResult.failedMessages)
            sendResult.onlySuccessful().forEach {
                assertEquals(it.message.data, it.id.shard)
            }
        }

        // Read from the specified shard using direct consumer
        val first = readData(dataSource)
        val second = readData(dataSourceFirstSchema)
        val third = readData(dataSourceSecondSchema)

        // Check that all messages have been distributed across all nodes
        assertEquals(messagesToSend, first.size + second.size + third.size)
    }

    @Test
    fun testMessagesDistribution_SingleShard() {
        val shard = 123

        val results = (1..messagesToSend).map {
            val sendRequest = SendRequest(listOf(SendMessage(shard)), SendOptions(shard = shard))
            clusterProducer.send(queue, sendRequest)
        }

        // Test that all messages were sent successfully and only to the specified shard
        results.forEach { sendResult ->
            assertEquals(0, sendResult.failedMessages)
            sendResult.onlySuccessful().forEach {
                assertEquals(shard, it.id.shard)
                assertEquals(it.message.data, it.id.shard)
            }
        }

        // Read from the specified shard using direct consumer
        val first = readData(dataSource)
        val second = readData(dataSourceFirstSchema)
        val third = readData(dataSourceSecondSchema)

        if (first.isNotEmpty()) {
            assertEquals(messagesToSend, first.size)
            assertTrue(second.isEmpty(), "$second")
            assertTrue(third.isEmpty(), "$third")
        } else if (second.isNotEmpty()) {
            assertEquals(messagesToSend, second.size)
            assertTrue(first.isEmpty(), "$first")
            assertTrue(third.isEmpty(), "$third")
        } else if (third.isNotEmpty()) {
            assertEquals(messagesToSend, third.size)
            assertTrue(first.isEmpty(), "$first")
            assertTrue(second.isEmpty(), "$second")
        } else {
            fail("No messages were received")
        }
    }

    @Test
    fun testMessagesDistribution_RandomShard() {
        val results = (1..messagesToSend).map {
            val shard = ShardStrategy.Random.getShard()
            val sendRequest = SendRequest(listOf(SendMessage(shard)), SendOptions(shard = shard))
            clusterProducer.send(queue, sendRequest)
        }

        // Test that all messages were sent successfully and only to the specified shard
        results.forEach { sendResult ->
            assertEquals(0, sendResult.failedMessages)
            sendResult.onlySuccessful().forEach {
                assertEquals(it.message.data, it.id.shard)
            }
        }


        // Read from the specified shard using direct consumer
        val first = readData(dataSource)
        val second = readData(dataSourceFirstSchema)
        val third = readData(dataSourceSecondSchema)

        assertEquals(messagesToSend, first.size + second.size + third.size)
    }

    private fun readData(dataSource: DataSource): List<Message<Int>> {
        val consumer = DatabaseConsumer(dataSource)
        val messages = consumer.receive(queue, messagesToSend)
        consumer.delete(queue, messages)
        return messages
    }

}

fun findDataSourceWithInitializedShard(dataSources: List<DataSource>): DataSource {
    dataSources.forEach { ds ->
        val shards = try {
            ShardSchema.readShards(ds)
        } catch (_: Exception) {
            // Shard table doesn't exist
            emptyMap()
        }

        // Shard table is 100% initialized
        if (shards.size == Shard.SHARD_COUNT) {
            return ds
        }
    }

    throw IllegalStateException("No fully initialized shard table found")
}
