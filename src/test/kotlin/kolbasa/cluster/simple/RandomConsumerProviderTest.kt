package kolbasa.cluster.simple

import kolbasa.AbstractPostgresqlTest
import kolbasa.producer.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kotlin.test.*
import kotlin.test.assertFailsWith

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
    fun testMessagesDistribution() {
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
    fun checkEmptyConsumerProvider() {
        assertFailsWith<IllegalStateException> {
            RandomConsumerProvider(emptyList())
        }
    }

}
