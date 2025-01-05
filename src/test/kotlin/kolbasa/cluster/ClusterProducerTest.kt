package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.Message
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.producer.SendRequest
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.sql.Statement
import javax.sql.DataSource
import kotlin.test.*

class ClusterProducerTest : AbstractPostgresqlTest() {

    private val messagesToSend = 1_000

    private val queue = Queue<Int, Unit>(
        name = "test",
        databaseDataType = PredefinedDataTypes.Int
    )

    private lateinit var cluster: Cluster
    private lateinit var clusterProducer: ClusterProducer<Int, Unit>

    @BeforeTest
    fun before() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

        dataSources.forEach { dataSource ->
            SchemaHelpers.updateDatabaseSchema(dataSource, queue)
        }

        cluster = Cluster(dataSources)
        cluster.updateState()

        clusterProducer = ClusterProducer(cluster, queue)
    }

    @Test
    fun testMessagesDistribution_SendOneMessageAndCheckShard() {
        val shard = 123

        val sendRequest = SendRequest<Int, Unit>(listOf(SendMessage(shard)), SendOptions(shard = shard))
        val sendResult = clusterProducer.send(sendRequest)

        // read directly from the producer node
        val producerNode = requireNotNull(cluster.getState().shards[shard]?.producerNode)
        val dataSource = requireNotNull(cluster.getState().nodes[producerNode])
        val consumer = DatabaseConsumer(dataSource, queue)
        val rawMessages = consumer.receive(messagesToSend)

        assertEquals(1, rawMessages.size)
        assertEquals(sendResult.onlySuccessful().map { it.id }, rawMessages.map { it.id })
        assertEquals(sendResult.onlySuccessful().map { it.message.data }, rawMessages.map { it.data })
    }

    @Test
    fun testMessagesDistribution_SendOneMessageIfProducerNodeFailed() {
        // change producer node to 'unknown' for this dataSource
        val id = requireNotNull(IdSchema.readNodeId(dataSource))
        listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema).forEach { ds ->
            try {
                ShardSchema.readShards(ds)
                // shard table found, let's change producer nodes
                ds.useStatement { statement: Statement ->
                    val sql = """
                       update
                            ${ShardSchema.SHARD_TABLE_NAME}
                       set
                            ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'unknown',
                            ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'unknown'
                       where
                            ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = '$id'
                   """.trimIndent()
                    statement.executeUpdate(sql)
                }
            } catch (e: Exception) {
                // NOP
            }
        }
        // re-read cluster state after shards changing
        cluster.updateState()

        // send to random nodes
        val results = (1..messagesToSend).map {
            val shard = ShardStrategy.Random.getShard()
            val sendRequest = SendRequest<Int, Unit>(listOf(SendMessage(shard)), SendOptions(shard = shard))
            clusterProducer.send(sendRequest)
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

        // Check that "non-existing"
        assertEquals(0, first.size)
        assertEquals(messagesToSend, second.size + third.size)
    }

    @Test
    fun testMessagesDistribution_SingleShard() {
        val shard = 123

        val results = (1..messagesToSend).map {
            val sendRequest = SendRequest<Int, Unit>(listOf(SendMessage(shard)), SendOptions(shard = shard))
            clusterProducer.send(sendRequest)
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
            val sendRequest = SendRequest<Int, Unit>(listOf(SendMessage(shard)), SendOptions(shard = shard))
            clusterProducer.send(sendRequest)
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

    private fun readData(dataSource: DataSource): List<Message<Int, Unit>> {
        val consumer = DatabaseConsumer(dataSource, queue)
        val messages = consumer.receive(messagesToSend)
        consumer.delete(messages)
        return messages
    }

}