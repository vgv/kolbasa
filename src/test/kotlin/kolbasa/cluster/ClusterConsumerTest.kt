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
import kolbasa.schema.SchemaHelpers
import java.sql.Statement
import javax.sql.DataSource
import kotlin.test.*

class ClusterConsumerTest : AbstractPostgresqlTest() {

    private val queue = Queue.of("test", PredefinedDataTypes.Int)

    private val dataSources by lazy { listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema) }
    private lateinit var cluster: Cluster
    private lateinit var clusterConsumer: ClusterConsumer

    @BeforeTest
    fun before() {
        Kolbasa.shardStrategy = ShardStrategy.Random

        dataSources.forEach { dataSource ->
            SchemaHelpers.createOrUpdateQueues(dataSource, queue)
        }

        cluster = Cluster(dataSources)
        cluster.updateStateOnce()

        clusterConsumer = ClusterConsumer(cluster)
    }

    @Test
    fun testReceive_JustReceiveTest() {
        // Send N messages
        val clusterProducer = ClusterProducer(cluster)
        (Shard.MIN_SHARD..Shard.MAX_SHARD).forEach { message ->
            clusterProducer.send(queue, message)
        }

        // Try to receive all messages and test that all messages are received
        val received = tryToReadEverything()
        assertEquals(Shard.SHARD_COUNT, received.size)
        assertEquals((Shard.MIN_SHARD..Shard.MAX_SHARD).toSet(), received.map { it.data }.toSet())
    }

    @Test
    fun testMessagesDistribution_TestOneMigratingShard() {
        // Send N messages randomly to all nodes
        val clusterProducer = ClusterProducer(cluster)
        (Shard.MIN_SHARD..Shard.MAX_SHARD).forEach { message ->
            val sendRequest = SendRequest(
                data = listOf(SendMessage(data = message)),
                sendOptions = SendOptions(shard = message)
            )

            clusterProducer.send(queue, sendRequest)
        }

        // change consumer node to 'unknown' for this dataSource
        val migratingShard = ShardStrategy.Random.getShard()
        findDataSourceWithInitializedShard(dataSources).useStatement { statement: Statement ->
            val sql = """
                       update
                            ${ShardSchema.SHARD_TABLE_NAME}
                       set
                            ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'unknown',
                            ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = null,
                            ${ShardSchema.NEXT_CONSUMER_NODE_COLUMN_NAME} = 'unknown'
                       where
                            ${ShardSchema.SHARD_COLUMN_NAME} = $migratingShard
                   """.trimIndent()
            statement.executeUpdate(sql)
        }
        // re-read cluster state after shards changing
        cluster.updateStateOnce()


        // Try to receive as many messages as possible
        val received = tryToReadEverything()
        // Check we read everything except one migrating shard
        assertEquals(Shard.SHARD_COUNT - 1, received.size, "received: $received")
        assertTrue(received.none { it.data == migratingShard }, "received: $received")

        // Read from the specified shard using direct consumer and check that there is only one message from "migrating" shard
        val first = readData(dataSource)
        val second = readData(dataSourceFirstSchema)
        val third = readData(dataSourceSecondSchema)
        assertEquals(1, first.size + second.size + third.size, "second: $second, third: $third")
    }

    private fun readData(dataSource: DataSource): List<Message<Int>> {
        val consumer = DatabaseConsumer(dataSource)
        val messages = consumer.receive(queue, Shard.SHARD_COUNT)
        consumer.delete(queue, messages)
        return messages
    }

    private fun tryToReadEverything(): List<Message<Int>> {
        val attemptsToRead = 100
        // Receive N messages and test that all messages are received
        val received = mutableListOf<Message<Int>>()

        var emptyReceiveAttempt = 0
        do {
            val messages = clusterConsumer.receive(queue, Shard.SHARD_COUNT)
            received += messages
            clusterConsumer.delete(queue, messages)

            if (messages.isEmpty()) {
                emptyReceiveAttempt++
                if (emptyReceiveAttempt > attemptsToRead) {
                    break
                }
            } else {
                emptyReceiveAttempt = 0
            }
        } while (true)

        return received
    }

}
