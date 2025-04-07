package examples

import kolbasa.consumer.ConsumerOptions
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.queue.DatabaseQueueDataType
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

data class MetaData(
    val correlationId: String,
)

data class EventData(
    val message: String
) : Serializable

object EventDataSerde {
    fun serialize(eventData: EventData): ByteArray =
        ByteArrayOutputStream().use { byteStream ->
            ObjectOutputStream(byteStream).use { objStream ->
                objStream.writeObject(eventData)
                objStream.flush()
                byteStream.toByteArray()
            }
        }

    fun deserialize(bytes: ByteArray): EventData =
        java.io.ByteArrayInputStream(bytes).use { byteStream ->
            java.io.ObjectInputStream(byteStream).use { objStream ->
                objStream.readObject() as EventData
            }
        }
}

class QueueContext<Data, Meta : Any>(
    name: String,
    eventType: DatabaseQueueDataType<Data>,
    metadataType: Class<Meta>
) {
    val queue = Queue(name, eventType, metadata = metadataType)
    val dataSource = ExamplesDataSourceProvider.getDataSource()

    init {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
    }
}

class EventDataProducer(
    private val queueContext: QueueContext<EventData, MetaData>
) {
    private val producer = DatabaseProducer(queueContext.dataSource)

    fun sendAsync(message: EventData): CompletableFuture<Unit> =
        producer.sendAsync(queueContext.queue, message)
            .thenApply { _ ->
                println("[${Thread.currentThread().name}] Message sent: ${message.message}")
            }
}

class EventDataConsumer(
    private val queueContext: QueueContext<EventData, MetaData>,
    private val consumerOptions: ConsumerOptions
) {
    private val consumer = DatabaseConsumer(queueContext.dataSource, consumerOptions)
    private val messageCounter = AtomicInteger(0)

    fun consumeAsync(processor: (EventData) -> Unit): CompletableFuture<EventData?> =
        CompletableFuture.supplyAsync {
            consumer.receive(queueContext.queue)?.let { message ->
                val eventData = message.data
                println("[${Thread.currentThread().name}] Processing message: ${eventData.message}")
                processor(eventData)
                messageCounter.incrementAndGet()
                eventData
            }
        }

    fun getMessageCount(): Int = messageCounter.get()
}

class AsyncStream() {
    private val dataType = DatabaseQueueDataType.Binary(
        serializer = EventDataSerde::serialize,
        deserializer = EventDataSerde::deserialize
    )
    private val queueContext = QueueContext("test_queue", dataType, MetaData::class.java)
    private val producer = EventDataProducer(queueContext)

    fun startProducerStream(numberOfMessages: Int): List<CompletableFuture<Void>> =
        (0 until numberOfMessages).map { i ->
            val delay = i * 100L // 100ms delay between each message
            CompletableFuture.runAsync({
                producer.sendAsync(EventData("Message $i")).join()
            }, CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS))
        }

    fun startConsumerStream(timeoutSeconds: Long, expectedMessages: Int, consumerName: String): CompletableFuture<Void> {
        val consumer = EventDataConsumer(queueContext,  ConsumerOptions(consumerName))

        return CompletableFuture.runAsync {
            println("[${Thread.currentThread().name}] [$consumerName] Starting consumer stream")
            val startTime = System.currentTimeMillis()

            while (true) {
                consumer.consumeAsync { eventData ->
                    println("[${Thread.currentThread().name}] [$consumerName] Received: ${eventData.message}")
                }.get(1, TimeUnit.SECONDS)

                if (consumer.getMessageCount() >= expectedMessages) {
                    println("[${Thread.currentThread().name}] [$consumerName] Consumer stream completed - received all messages")
                    break
                }

                if (System.currentTimeMillis() - startTime > timeoutSeconds * 1000) {
                    println("[${Thread.currentThread().name}] [$consumerName] Consumer stream timed out. Messages consumed [${consumer.getMessageCount()}]")
                    break
                }

                Thread.sleep(200)
            }
        }
    }
}

fun main() {
    val asyncStream = AsyncStream()
    val numberOfMessages = 100
    val timeoutSeconds = 30L

    asyncStream.startProducerStream(numberOfMessages)
    val consumerFuture = asyncStream.startConsumerStream(timeoutSeconds, numberOfMessages, "FirstConsumer")

    val consumerSecond = asyncStream.startConsumerStream(timeoutSeconds, numberOfMessages, "SecondConsumer")

    CompletableFuture.allOf(consumerFuture, consumerSecond).join()
    println("[${Thread.currentThread().name}] All operations completed")
}
