package kolbasa.cluster.migrate

import io.mockk.mockk
import io.mockk.verifySequence
import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.Id
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult.Companion.onlySuccessful
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.Queue
import kolbasa.schema.SchemaExtractor
import kolbasa.schema.SchemaHelpers
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.random.Random
import kotlin.test.*

internal class MigrateOneTableTest : AbstractPostgresqlTest() {

    val itemsToMove = 20_000
    val migrateBatchSize = 1000
    val shardsToMove = listOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val shardsToStay = listOf(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    var sentItems = mutableMapOf<Id, SendMessage<Value, TestMeta>>()

    @BeforeTest
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
        SchemaHelpers.updateDatabaseSchema(dataSourceFirstSchema, queue)

        // shard => messages to send
        val dataToSend = (1..itemsToMove)
            .map { _ ->
                val shard = if (Random.nextBoolean()) {
                    shardsToMove.random()
                } else {
                    shardsToStay.random()
                }

                SendMessage<Value, TestMeta>(data = Value(shard), meta = randomTestMeta())
            }
            .groupBy { it.data.value }


        // send messages to the source database
        val producer = DatabaseProducer(dataSource)
        dataToSend.forEach { (shard, messages) ->
            val request = SendRequest(messages, SendOptions(shard = shard))
            request.effectiveShard = shard
            val (failedCount, result) = producer.send(queue, request)

            assertEquals(0, failedCount)
            result.onlySuccessful().forEach {
                sentItems[it.id] = it.message
            }
        }

        // check that all messages were sent
        assertEquals(itemsToMove, sentItems.size)
    }

    @Test
    fun migrate() {
        // Just to check before migration that the target database is empty
        val consumer = DatabaseConsumer(dataSourceFirstSchema)
        consumer.receive(queue)?.let { message ->
            fail("There should be no messages in the target database, but found at least one: $message")
        }

        // start migration
        val schema = SchemaExtractor
            .extractRawSchema(dataSource, setOf(queue.dbTableName))
            .values
            .first()

        val callback = mockk<MigrateEvents>(relaxed = true)

        val migrateOneTable = MigrateOneTable(
            shards = shardsToMove,
            schema = schema,
            sourceDataSource = dataSource,
            targetDataSource = dataSourceFirstSchema,
            rowsPerBatch = migrateBatchSize,
            moveProgressCallback = callback
        )
        val migratedItems = migrateOneTable.migrate()

        // after migration check that all required messages with (shard=shardsToMove) were moved
        var foundItems = 0
        do {
            val messages = consumer.receive(queue, limit = 100, receiveOptions = ReceiveOptions(readMetadata = true))
            messages.forEach { message ->
                // check that the shard is in the list of shards to move
                val shard = message.data.value
                assertTrue(shardsToMove.contains(shard), "Shard $shard should be in the list of shards to move: $shardsToMove")

                // check that the meta fields were moved correctly
                val originalMsg = requireNotNull(sentItems[message.id])
                assertEquals(originalMsg.meta, message.meta)
            }
            consumer.delete(queue, messages)

            foundItems += messages.size
        } while (messages.isNotEmpty())

        assertEquals(migratedItems, foundItems)

        // check that the callback was called correctly
        verifySequence {
            callback.migrateStart(queue.dbTableName, dataSource, dataSourceFirstSchema)

            var totalRows = 0
            while (totalRows < migratedItems) {
                totalRows += migrateBatchSize
                if (totalRows > migratedItems) {
                    totalRows = migratedItems
                }
                callback.migrateNextBatch(queue.dbTableName, dataSource, dataSourceFirstSchema, totalRows)
            }
            callback.migrateNextBatch(queue.dbTableName, dataSource, dataSourceFirstSchema, migratedItems)

            callback.migrateEnd(queue.dbTableName, dataSource, dataSourceFirstSchema, migratedItems)
        }
    }

    companion object {
        internal enum class TestEnum {
            FIRST,
            SECOND,
            THIRD
        }

        internal data class TestMeta(
            val stringField: String,
            val longField: Long,
            val intField: Int,
            val shortField: Short,
            val byteField: Byte,
            val booleanField: Boolean,
            val doubleField: Double,
            val floatField: Float,
            val charField: Char,
            val bigIntegerField: BigInteger,
            val bigDecimalField: BigDecimal,
            val enumField: TestEnum
        )

        internal data class Value(val value: Int)

        internal val queue = Queue<Value, TestMeta>(
            "test_queue",
            DatabaseQueueDataType.Json<Value>(
                serializer = { "{\"value\": ${it.value}}" },
                deserializer = { Value(it.removeSurrounding("{\"value\": ", "}").toInt()) }),
            metadata = TestMeta::class.java
        )

        internal fun randomTestMeta(): TestMeta {
            return TestMeta(
                stringField = ('a'..'z').toList().shuffled().take(Random.nextInt(5, 10)).joinToString(""),
                longField = Random.nextLong(),
                intField = Random.nextInt(),
                shortField = Random.nextInt().toShort(),
                byteField = Random.nextInt().toByte(),
                booleanField = Random.nextBoolean(),
                doubleField = Random.nextDouble(Double.MIN_VALUE, Double.MAX_VALUE),
                floatField = Random.nextFloat() * 1_000_000,
                charField = ('a'..'z').random(),
                bigIntegerField = BigInteger.valueOf(Random.nextLong()),
                bigDecimalField = BigDecimal.valueOf(Random.nextDouble()),
                enumField = TestEnum.values().random()
            )
        }
    }

}
