package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import kolbasa.Kolbasa
import kolbasa.assertNotNull
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.inspector.CountOptions
import kolbasa.inspector.DistinctValuesOptions
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import java.time.Duration

class ClusterInspectorTest : AbstractPostgresqlTest() {

    private val FIELD = MetaField.int("field", FieldOption.SEARCH)

    private val queue = Queue.of(
        "test",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIELD)
    )

    private val dataSources by lazy { listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema) }
    private lateinit var cluster: Cluster
    private lateinit var clusterInspector: ClusterInspector
    private lateinit var clusterProducer: ClusterProducer

    @BeforeEach
    fun before() {
        Kolbasa.shardStrategy = ShardStrategy.Random

        dataSources.forEach { ds ->
            SchemaHelpers.createOrUpdateQueues(ds, queue)
        }

        cluster = Cluster(dataSources)
        cluster.updateStateOnce()

        clusterInspector = ClusterInspector(cluster)
        clusterProducer = ClusterProducer(cluster)
    }

    @Test
    fun testCount_EmptyCluster() {
        val messages = clusterInspector.count(queue, CountOptions(samplePercent = 100f))

        assertEquals(0, messages.total())
    }

    @Test
    fun testCount_AggregatesAcrossNodes() {
        val scheduledMessages = 1000
        val readyMessages = 2000

        (1..scheduledMessages + readyMessages).forEach { i ->
            val options = if (i <= scheduledMessages)
                MessageOptions(delay = Duration.ofMinutes(10))
            else
                MessageOptions(delay = Duration.ZERO)

            clusterProducer.send(queue, SendMessage(i.toString(), MetaValues.of(FIELD.value(i)), messageOptions = options))
        }

        val messages = clusterInspector.count(queue, CountOptions(samplePercent = 100f))

        assertEquals(scheduledMessages.toLong() + readyMessages, messages.total())
        assertEquals(scheduledMessages.toLong(), messages.scheduled)
        assertEquals(readyMessages.toLong(), messages.ready)
    }

    @Test
    fun testDistinctValues_MergesAcrossNodes() {
        // 100 equal lists of 20 values each (1..20), shuffled across the cluster → each value appears exactly 100 times in total
        val items = 20
        val duplicates = 100
        val values = (1..duplicates).flatMap { (1..items).toList() }

        values.forEachIndexed { index, value ->
            clusterProducer.send(queue, SendMessage(index.toString(), MetaValues.of(FIELD.value(value))))
        }

        val result = clusterInspector.distinctValues(queue, FIELD, items + 1, DistinctValuesOptions(samplePercent = 100f))

        // All 3 distinct values should be present
        assertEquals((1..items).toSet(), result.keys)
        // Each value appears 'duplicates' times
        (1..items).forEach { item ->
            assertEquals(duplicates.toLong(), result[item])
        }
    }

    @Test
    fun testSize() {
        var size = clusterInspector.size(queue)
        assertTrue(size > 0) { "Size should be zero, because table is empty, got $size" }

        // Put some data in the cluster
        (1..1000).forEach { i ->
            clusterProducer.send(queue, SendMessage(i.toString(), MetaValues.of(FIELD.value(i))))
        }

        // Check size again, it should be greater than zero now
        size = clusterInspector.size(queue)
        assertTrue(size > 0) { "Size should be positive, got $size" }
    }

    @Test
    fun testIsEmpty() {
        // Initially, the cluster should be empty
        assertTrue(clusterInspector.isEmpty(queue))

        // Put some data in the cluster
        clusterProducer.send(queue, SendMessage("data", MetaValues.of(FIELD.value(1))))

        // Check isEmpty again, it should be false now
        assertFalse(clusterInspector.isEmpty(queue))
    }

    @Test
    fun testIsDeadOrEmpty_TrueWhenAllDeadOrEmpty() {
        val messages = 1000

        // Send messages with 1 attempt to all nodes, then receive with zero visibility → DEAD
        (1..messages).forEach { i ->
            clusterProducer.send(
                queue,
                SendMessage(i.toString(), MetaValues.of(FIELD.value(i)), MessageOptions(attempts = 1))
            )
        }

        // Initially, messages are not dead yet, because they have not been received and their attempts has not been exhausted
        assertFalse(clusterInspector.isDeadOrEmpty(queue))
        assertFalse(clusterInspector.isEmpty(queue))

        // Receive from each node directly to make messages dead
        dataSources.forEach { ds ->
            val consumer = DatabaseConsumer(ds)
            consumer.receive(queue, limit = messages, ReceiveOptions(visibilityTimeout = Duration.ZERO))
        }

        // Now all messages should be dead, so isDeadOrEmpty should return true, but isEmpty should return false, because
        // messages are still present in the cluster, just marked as dead.
        // This is the subtle but important difference between isDeadOrEmpty and isEmpty - the first one checks if all nodes
        // report the queue as dead or empty, while the second one checks if all nodes report the queue as empty. In this case,
        // all nodes report the queue as dead, but not empty, so isDeadOrEmpty should return true, but isEmpty should return false
        assertTrue(clusterInspector.isDeadOrEmpty(queue))
        assertFalse(clusterInspector.isEmpty(queue))
    }

    @Test
    fun testMessageAge_EmptyCluster() {
        val age = clusterInspector.messageAge(queue)

        assertNull(age.oldest)
        assertNull(age.newest)
        assertNull(age.oldestReady)
    }

    @Test
    fun testMessageAge_WithMessages() {
        (1..1000).forEach { i ->
            clusterProducer.send(queue, SendMessage(i.toString(), MetaValues.of(FIELD.value(i))))
        }

        val age = clusterInspector.messageAge(queue)

        assertTrue(assertNotNull(age.oldest).toMillis() >= 0)
        assertTrue(assertNotNull(age.newest).toMillis() >= 0)
        assertTrue(assertNotNull(age.oldestReady).toMillis() >= 0)
    }
}
