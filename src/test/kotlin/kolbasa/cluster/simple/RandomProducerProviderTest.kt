package kolbasa.cluster.simple

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import javax.sql.DataSource
import kotlin.test.*

class RandomProducerProviderTest : AbstractPostgresqlTest() {

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
    fun checkEmptyProducerProvider() {
        assertFailsWith<IllegalStateException> {
            RandomProducerProvider(emptyList())
        }
    }

    @Test
    fun testMessagesDistribution_Random() {
        val producerProvider = RandomProducerProvider(
            dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        )

        // Send all messages to the queue using 3 producers
        (1..messagesToSend).forEach {
            producerProvider.producer(queue).send(it)
        }

        // Read from these three servers using direct consumers
        val firstIds = readInts(dataSource)
        val secondIds = readInts(dataSourceFirstSchema)
        val thirdIds = readInts(dataSourceSecondSchema)

        // Check that all messages were distributed between three servers
        assertTrue(firstIds.isNotEmpty() && firstIds.size < messagesToSend, "$firstIds")
        assertTrue(secondIds.isNotEmpty() && secondIds.size < messagesToSend, "$secondIds")
        assertTrue(thirdIds.isNotEmpty() && thirdIds.size < messagesToSend, "$thirdIds")

        // Check that all messages were received
        assertEquals(messagesToSend, (firstIds + secondIds + thirdIds).toSet().size)
    }

    @Test
    fun testMessagesDistribution_ByShard() {
        val producerProvider = RandomProducerProvider(
            dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        )

        // Send all messages to the queue using 3 producers
        (1..messagesToSend).forEach {
            producerProvider.producer(queue, it).send(it)
        }

        // Read from these three servers using direct consumers
        val firstIds = readInts(dataSource)
        val secondIds = readInts(dataSourceFirstSchema)
        val thirdIds = readInts(dataSourceSecondSchema)

        // Check that all messages were distributed between three servers
        assertTrue(firstIds.isNotEmpty() && firstIds.size < messagesToSend, "$firstIds")
        assertTrue(secondIds.isNotEmpty() && secondIds.size < messagesToSend, "$secondIds")
        assertTrue(thirdIds.isNotEmpty() && thirdIds.size < messagesToSend, "$thirdIds")

        // Check shard distribution
        // All ids belong to the same shard
        assertTrue(firstIds.map { it % 3 }.toSet().size == 1, "$firstIds")
        assertTrue(secondIds.map { it % 3 }.toSet().size == 1, "$secondIds")
        assertTrue(thirdIds.map { it % 3 }.toSet().size == 1, "$thirdIds")

        // Check that all messages were received
        assertEquals(messagesToSend, (firstIds + secondIds + thirdIds).toSet().size)
    }


    private fun readInts(dataSource: DataSource): List<Int> {
        val consumer = DatabaseConsumer(dataSource, queue)
        val messages = consumer.receive(messagesToSend)
        consumer.delete(messages)
        return messages.map { it.data }
    }

}
