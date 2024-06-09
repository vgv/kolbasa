package kolbasa.cluster.simple

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.datasource.Consumer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kotlin.test.*

class RandomConsumerProviderTest : AbstractPostgresqlTest() {

    private val messagesToSend = 1000

    private val queue = Queue<Int, Unit>(
        name = "test",
        databaseDataType = PredefinedDataTypes.Int
    )

    @BeforeTest
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
        SchemaHelpers.updateDatabaseSchema(dataSourceFirstSchema, queue)
        SchemaHelpers.updateDatabaseSchema(dataSourceSecondSchema, queue)
    }

    @Test
    fun checkEmptyConsumerProvider() {
        assertFailsWith<IllegalStateException> {
            RandomConsumerProvider(emptyList())
        }
    }

    @Test
    fun testMessagesDistribution_Random() {
        // Send all messages to the queue using 3 direct producers
        val producers = listOf(
            DatabaseProducer(dataSource, queue),
            DatabaseProducer(dataSourceFirstSchema, queue),
            DatabaseProducer(dataSourceSecondSchema, queue)
        )
        (1..messagesToSend).forEach {
            producers.random().send(it)
        }

        // Read from these three servers using direct consumers
        val consumerProvider = RandomConsumerProvider(
            dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        )

        val readIds = mutableSetOf<Int>()
        // The value 100 doesn't really matter, it's just the number of iterations, which I hope will be enough
        // to read all the messages from all the servers, since we only have 3 servers
        (1..100).forEach { _ ->
            val consumer = consumerProvider.consumer(queue)
            val messages = consumer.receive(messagesToSend)
            consumer.delete(messages)
            readIds.addAll(messages.map { it.data })
        }

        assertEquals(messagesToSend, readIds.size)
    }

    @Test
    fun testMessagesDistribution_ByShard() {
        // Read from these three servers using direct consumers
        val consumerProvider = RandomConsumerProvider(
            dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        )

        // Check consumer distribution
        val consumers = mutableMapOf<Consumer<*, *>, Int>()
        (1..1000).forEach { _ ->
            val consumer = consumerProvider.consumer(queue, 123) // 123 means nothing, just constant value, constant shard
            consumers.compute(consumer) { _, current ->
                if (current == null) {
                    1
                } else {
                    current + 1
                }
            }
        }

        // check consumers distribution
        val largest = consumers.values.max()
        val smallest = consumers.values.min()

        assertTrue(largest > smallest * RandomConsumerProvider.RANDOM_CONSUMER_PROBABILITY, "Largest: $largest, smalles: $smallest")
    }

}
