package kolbasa.worker

import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterProducer
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.producer.PartialInsert
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendOptions
import kolbasa.producer.SendRequest
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.readInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.SQLException
import javax.sql.DataSource

class QueuePipeTest : AbstractPostgreSQLTest() {

    private val from = Queue(
        name = "pipe_from",
        databaseDataType = PredefinedDataTypes.String,
        options = QueueOptions(defaultAttempts = 5)
    )

    private val to = Queue(
        name = "pipe_to",
        databaseDataType = PredefinedDataTypes.String
    )

    // The source queue and the target queue live in different databases, as they would on different servers
    private val fromDataSource by lazy { dataSource }
    private val toDataSource by lazy { dataSourceFirstSchema }

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(fromDataSource, from)
        SchemaHelpers.createOrUpdateQueues(toDataSource, to)
    }

    @Test
    fun testProcess_Empty() {
        val pipe = pipe { error("No messages, nothing to transform") }

        val result = pipe.process()
        assertEquals(ProcessResult(succeeded = 0, skipped = 0, failed = 0, requested = 10), result)
        assertEquals(0, result.received)
    }

    @Test
    fun testProcess_MovesMessages() {
        send("one", "two", "three")

        val pipe = pipe { message -> SendMessage(message.data.uppercase()) }

        assertEquals(from, pipe.queue)
        assertEquals(ProcessResult(succeeded = 3, skipped = 0, failed = 0, requested = 10), pipe.process())
        assertEquals(0, messagesIn(fromDataSource, from))
        assertEquals(setOf("ONE", "TWO", "THREE"), receiveAll(toDataSource))
    }

    @Test
    fun testProcess_SameMessageForTwoSources() {
        send("a", "b")

        // The same object for both source messages still gives two messages
        val same = SendMessage("same")
        val pipe = pipe { same }

        assertEquals(ProcessResult(succeeded = 2, skipped = 0, failed = 0, requested = 10), pipe.process())
        assertEquals(0, messagesIn(fromDataSource, from))
        assertEquals(2, messagesIn(toDataSource, to))
    }

    @Test
    fun testProcess_TransformFails() {
        send("good", "bad")

        val errors = mutableListOf<ProcessError>()
        val options = QueuePipeOptions(errorHandler = { errors += it })
        val pipe = pipe(options) { message ->
            if (message.data == "bad") throw IllegalArgumentException("Can't transform '${message.data}'")
            SendMessage(message.data)
        }

        assertEquals(ProcessResult(succeeded = 1, skipped = 0, failed = 1, requested = 10), pipe.process())

        // The error handler got the failed message
        val error = errors.single()
        assertEquals(from, error.queue)
        assertEquals("bad", error.message.data)
        assertInstanceOf(IllegalArgumentException::class.java, error.cause)

        // The good message moved, the bad one stays in the source queue with one attempt spent
        assertEquals(setOf("good"), receiveAll(toDataSource))
        assertEquals(1, messagesIn(fromDataSource, from))
        assertEquals(4, fromDataSource.readInt("select remaining_attempts from ${from.dbTableName}"))
    }

    @Test
    fun testProcess_SendFails() {
        send("one", "two")

        // No target table, so the send fails
        SchemaHelpers.deleteQueues(toDataSource, to)

        val pipe = pipe { message -> SendMessage(message.data) }
        assertThrows<SQLException> { pipe.process() }

        // Nothing is deleted, every message has one attempt spent
        assertEquals(2, messagesIn(fromDataSource, from))
        assertEquals(2, fromDataSource.readInt("select count(*) from ${from.dbTableName} where remaining_attempts = 4"))
    }

    @Test
    fun testProcess_AllOrNothing() {
        val unique = uniqueQueueWithKey2()
        send("1", "2", "3")

        // This producer would insert what it can, one message per INSERT. The pipe sends all or nothing anyway
        val producerOptions = ProducerOptions(chunkSize = 1, partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)
        val producer = DatabaseProducer(toDataSource, producerOptions)
        val pipe = QueuePipe.map(from, DatabaseConsumer(fromDataSource), unique, producer) { message ->
            SendMessage(message.data, MetaValues.of(KEY.value(message.data.toInt())))
        }
        assertThrows<SQLException> { pipe.process() }

        // Key 2 failed the whole batch: nothing was sent, nothing was deleted
        assertEquals(1, messagesIn(toDataSource, unique))
        assertEquals(3, messagesIn(fromDataSource, from))
    }

    @Test
    fun testProcess_ToCluster() {
        val data = (1..10).map { "message_$it" }
        send(*data.toTypedArray())

        val pipe = QueuePipe.map(from, DatabaseConsumer(fromDataSource), to, clusterProducer()) { message ->
            SendMessage(message.data)
        }

        assertEquals(ProcessResult(succeeded = 10, skipped = 0, failed = 0, requested = 10), pipe.process())
        assertEquals(0, messagesIn(fromDataSource, from))
        assertEquals(data.toSet(), clusterNodes.flatMap { receiveAll(it) }.toSet())
    }

    @Test
    fun testProcess_ProducerOptions() {
        send("one", "two", "three")

        // The pipe sets only the partial insert, the rest comes from the producer. A shard means something only in a
        // cluster: a single node stores every message with shard 0
        val producer = clusterProducer(ProducerOptions(shard = 42))
        val pipe = QueuePipe.map(from, DatabaseConsumer(fromDataSource), to, producer) { message ->
            SendMessage(message.data)
        }

        assertEquals(ProcessResult(succeeded = 3, skipped = 0, failed = 0, requested = 10), pipe.process())
        val withShard = clusterNodes.sumOf { it.readInt("select count(*) from ${to.dbTableName} where shard = 42") }
        assertEquals(3, withShard)
    }

    @Test
    fun testFlatMap_ManyAndNone() {
        send("a,b,c", "drop", "d")

        val pipe = flatMapPipe { message ->
            if (message.data == "drop") emptyList() else message.data.split(",").map { SendMessage(it) }
        }

        // The filtered message is deleted too, it just sends nothing
        assertEquals(ProcessResult(succeeded = 3, skipped = 0, failed = 0, requested = 10), pipe.process())
        assertEquals(0, messagesIn(fromDataSource, from))
        assertEquals(4, messagesIn(toDataSource, to))
        assertEquals(setOf("a", "b", "c", "d"), receiveAll(toDataSource))
    }

    @Test
    fun testFlatMap_AllFiltered_NoSend() {
        send("one", "two")

        // No target table: any send would fail, so the pipe must not send at all
        SchemaHelpers.deleteQueues(toDataSource, to)

        val pipe = flatMapPipe { emptyList() }

        assertEquals(ProcessResult(succeeded = 2, skipped = 0, failed = 0, requested = 10), pipe.process())
        assertEquals(0, messagesIn(fromDataSource, from))
    }

    @Test
    fun testFlatMap_SendFails_KeepsFiltered() {
        send("keep", "drop")

        // No target table, so the send fails
        SchemaHelpers.deleteQueues(toDataSource, to)

        val pipe = flatMapPipe { message ->
            if (message.data == "drop") emptyList() else listOf(SendMessage(message.data))
        }
        assertThrows<SQLException> { pipe.process() }

        // The filtered message stays too: the whole batch is transformed again next time
        assertEquals(2, messagesIn(fromDataSource, from))
    }

    @Test
    fun testMapBatch_OneMessageWithShard() {
        send("one", "two", "three")

        // The whole batch becomes one message, and the batch chooses its shard. A shard means something only in a
        // cluster
        val pipe = QueuePipe.mapBatch(from, DatabaseConsumer(fromDataSource), to, clusterProducer()) { batch ->
            val merged = batch.map { it.data }.sorted().joinToString("+")
            SendRequest(listOf(SendMessage(merged)), SendOptions(shard = 42))
        }

        assertEquals(ProcessResult(succeeded = 3, skipped = 0, failed = 0, requested = 10), pipe.process())
        assertEquals(0, messagesIn(fromDataSource, from))
        val withShard = clusterNodes.sumOf { it.readInt("select count(*) from ${to.dbTableName} where shard = 42") }
        assertEquals(1, withShard)
        assertEquals(setOf("one+three+two"), clusterNodes.flatMap { receiveAll(it) }.toSet())
    }

    @Test
    fun testMapBatch_PartialInsertReplaced() {
        val unique = uniqueQueueWithKey2()
        send("1", "2", "3")

        // The request asks to insert what it can, one message per INSERT. The pipe sends all or nothing anyway
        val requestOptions = SendOptions(chunkSize = 1, partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)
        val producer = DatabaseProducer(toDataSource)
        val pipe = QueuePipe.mapBatch(from, DatabaseConsumer(fromDataSource), unique, producer) { batch ->
            SendRequest(batch.map { SendMessage(it.data, MetaValues.of(KEY.value(it.data.toInt()))) }, requestOptions)
        }
        assertThrows<SQLException> { pipe.process() }

        // Key 2 failed the whole batch: nothing was sent, nothing was deleted
        assertEquals(1, messagesIn(toDataSource, unique))
        assertEquals(3, messagesIn(fromDataSource, from))
    }

    @Test
    fun testMapBatch_TransformFails() {
        send("one", "two")

        val errors = mutableListOf<ProcessError>()
        val options = QueuePipeOptions(errorHandler = { errors += it })
        val pipe = mapBatchPipe(options) { throw IllegalArgumentException("Can't transform the batch") }

        assertEquals(ProcessResult(succeeded = 0, skipped = 0, failed = 2, requested = 10), pipe.process())

        // Every message of the batch went to the error handler with the same cause
        assertEquals(setOf("one", "two"), errors.map { it.message.data }.toSet())
        assertEquals(1, errors.map { it.cause }.distinct().size)
        assertInstanceOf(IllegalArgumentException::class.java, errors.first().cause)

        // Nothing was sent, both messages stay with one attempt spent
        assertEquals(0, messagesIn(toDataSource, to))
        assertEquals(2, fromDataSource.readInt("select count(*) from ${from.dbTableName} where remaining_attempts = 4"))
    }

    @Test
    fun testMapBatch_EmptyRequest_NoSend() {
        send("one", "two")

        // No target table: any send would fail, so the pipe must not send at all
        SchemaHelpers.deleteQueues(toDataSource, to)

        val pipe = mapBatchPipe { SendRequest(emptyList()) }

        assertEquals(ProcessResult(succeeded = 2, skipped = 0, failed = 0, requested = 10), pipe.process())
        assertEquals(0, messagesIn(fromDataSource, from))
    }

    @Test
    fun testAsIs_DoesNotReadTheData() {
        // A JSON queue whose class knows only the field "x", and whose deserializer can't read anything at all
        val pointType = DatabaseQueueDataType.Json<Point>(
            serializer = { """{"x": ${it.x}}""" },
            deserializer = { error("asIs() must not read the data") }
        )
        val fromPoints = Queue("pipe_points_from", pointType)
        val toPoints = Queue("pipe_points_to", pointType)
        SchemaHelpers.createOrUpdateQueues(fromDataSource, fromPoints)
        SchemaHelpers.createOrUpdateQueues(toDataSource, toPoints)

        // A message with a field that the class doesn't know, written past the serializer
        val json = """{"x": 1, "extra": "kept"}"""
        DatabaseProducer(fromDataSource).send(Queue("pipe_points_from", RAW_JSON), json).throwExceptionIfAny()

        val consumer = DatabaseConsumer(fromDataSource)
        val pipe = QueuePipe.asIs(fromPoints, consumer, toPoints, DatabaseProducer(toDataSource))
        assertEquals(fromPoints, pipe.queue)
        assertEquals(ProcessResult(succeeded = 1, skipped = 0, failed = 0, requested = 100), pipe.process())

        // The message moved as it was, with the unknown field
        val moved = DatabaseConsumer(toDataSource).receive(Queue("pipe_points_to", RAW_JSON))
        assertEquals(json, moved?.data)
    }

    @Test
    fun testAsIs_MovesMetadata() {
        val userId = MetaField.ofInt("user_id")
        val fromWithMeta = Queue("pipe_meta_from", PredefinedDataTypes.String, metadata = Metadata.of(userId))
        val toWithMeta = Queue("pipe_meta_to", PredefinedDataTypes.String, metadata = Metadata.of(userId))
        SchemaHelpers.createOrUpdateQueues(fromDataSource, fromWithMeta)
        SchemaHelpers.createOrUpdateQueues(toDataSource, toWithMeta)

        DatabaseProducer(fromDataSource)
            .send(fromWithMeta, SendMessage("bugaga", MetaValues.of(userId.value(42))))
            .throwExceptionIfAny()

        val consumer = DatabaseConsumer(fromDataSource)
        val pipe = QueuePipe.asIs(fromWithMeta, consumer, toWithMeta, DatabaseProducer(toDataSource))
        assertEquals(ProcessResult(succeeded = 1, skipped = 0, failed = 0, requested = 100), pipe.process())

        val moved = DatabaseConsumer(toDataSource).receive(toWithMeta, ReceiveOptions(readMetadata = true))
        assertEquals("bugaga", moved?.data)
        assertEquals(42, moved?.meta?.get(userId))
    }

    @Test
    fun testAsIs_DifferentColumns_Fails() {
        // Both queues hold strings, but one stores them as varchar and the other as jsonb
        val json = Queue("pipe_json", RAW_JSON)

        assertThrows<IllegalArgumentException> {
            QueuePipe.asIs(from, DatabaseConsumer(fromDataSource), json, DatabaseProducer(toDataSource))
        }
    }

    @Test
    fun testOptions_ReceiveLimit_DependsOnTheFactory() {
        send(*(1..15).map { "message_$it" }.toTypedArray())
        val consumer = DatabaseConsumer(fromDataSource)
        val producer = DatabaseProducer(toDataSource)

        // Not set: careful for a transform, which may be slow
        val mapPipe = QueuePipe.map(from, consumer, to, producer) { SendMessage(it.data) }
        assertEquals(ProcessResult(succeeded = 10, skipped = 0, failed = 0, requested = 10), mapPipe.process())

        // Not set: larger for asIs, which does almost nothing with a message
        val asIsPipe = QueuePipe.asIs(from, consumer, to, producer)
        assertEquals(ProcessResult(succeeded = 5, skipped = 0, failed = 0, requested = 100), asIsPipe.process())

        // Set: the same for every factory
        val options = QueuePipeOptions(receiveLimit = 50)
        assertEquals(50, QueuePipe.asIs(from, consumer, to, producer, options).process().requested)
        val mapWithOptions = QueuePipe.map(from, consumer, to, producer, options) { SendMessage(it.data) }
        assertEquals(50, mapWithOptions.process().requested)
    }

    @Test
    fun testOptions_ReceiveLimit() {
        assertThrows<IllegalArgumentException> { QueuePipeOptions(receiveLimit = 0) }
    }

    private fun pipe(
        options: QueuePipeOptions = QueuePipeOptions.DEFAULT,
        transform: MapTransform<String, String>
    ): QueuePipe {
        val consumer = DatabaseConsumer(fromDataSource)
        return QueuePipe.map(from, consumer, to, DatabaseProducer(toDataSource), options, transform)
    }

    private fun flatMapPipe(transform: FlatMapTransform<String, String>): QueuePipe {
        val consumer = DatabaseConsumer(fromDataSource)
        return QueuePipe.flatMap(from, consumer, to, DatabaseProducer(toDataSource), transform = transform)
    }

    private fun mapBatchPipe(
        options: QueuePipeOptions = QueuePipeOptions.DEFAULT,
        transform: BatchTransform<String, String>
    ): QueuePipe {
        val consumer = DatabaseConsumer(fromDataSource)
        return QueuePipe.mapBatch(from, consumer, to, DatabaseProducer(toDataSource), options, transform)
    }

    // A target queue that takes every key once, and already has key 2
    private fun uniqueQueueWithKey2(): Queue<String> {
        val unique = Queue("pipe_unique", PredefinedDataTypes.String, metadata = Metadata.of(KEY))
        SchemaHelpers.createOrUpdateQueues(toDataSource, unique)
        DatabaseProducer(toDataSource).send(unique, SendMessage("2", MetaValues.of(KEY.value(2)))).throwExceptionIfAny()
        return unique
    }

    // The target queue as a cluster of two nodes
    private val clusterNodes by lazy { listOf(dataSourceFirstSchema, dataSourceSecondSchema) }

    private fun clusterProducer(options: ProducerOptions = ProducerOptions.DEFAULT): ClusterProducer {
        clusterNodes.forEach { SchemaHelpers.createOrUpdateQueues(it, to) }
        val cluster = Cluster(clusterNodes)
        cluster.updateStateOnce()
        return ClusterProducer(cluster, options)
    }

    private fun send(vararg data: String) {
        DatabaseProducer(fromDataSource).send(from, data.map { SendMessage(it) }).throwExceptionIfAny()
    }

    private fun receiveAll(dataSource: DataSource): Set<String> {
        return DatabaseConsumer(dataSource).receive(to, 1000).map { it.data }.toSet()
    }

    private fun messagesIn(dataSource: DataSource, queue: Queue<*>): Int {
        return dataSource.readInt("select count(*) from ${queue.dbTableName}")
    }

    private data class Point(val x: Int)

    private companion object {
        // A string that goes to a jsonb column and back as it is
        val RAW_JSON = DatabaseQueueDataType.Json<String>({ it }, { it })

        // The key of a queue that takes every key once
        val KEY = MetaField.ofInt("key", FieldOption.ALL_LIVE_UNIQUE)
    }
}
