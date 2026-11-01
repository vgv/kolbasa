package kolbasa.telemetry.prometheus

import io.prometheus.metrics.model.registry.PrometheusRegistry
import io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot
import io.prometheus.metrics.model.snapshots.DataPointSnapshot
import io.prometheus.metrics.model.snapshots.DistributionDataPointSnapshot
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot
import kolbasa.Kolbasa
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.sweep.SweepHelper
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.inTransaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Isolated

// Kolbasa.prometheusConfig is global, so no other test may run at the same time
@Isolated
class PrometheusMetricsTest : AbstractPostgreSQLTest() {

    // A new registry for every test, so a test sees only its own metrics
    private val registry = PrometheusRegistry()

    @BeforeEach
    fun setPrometheusConfig() {
        Kolbasa.prometheusConfig = PrometheusConfig.Config(registry = registry)
    }

    @AfterEach
    fun resetPrometheusConfig() {
        Kolbasa.prometheusConfig = PrometheusConfig.None
    }

    @Test
    fun testSend() {
        val queue = createQueue("metrics_send")

        DatabaseProducer(dataSource).send(queue, listOf(SendMessage("first"), SendMessage("second")))

        // The partial insert mode is a label too, this is the default one
        val mode = "partial_insert_type" to "UNTIL_FIRST_FAILURE"
        assertEquals(1.0, value("kolbasa_producer_send", queue, mode))
        assertEquals(2.0, value("kolbasa_producer_send_messages", queue, mode))
        assertEquals(0.0, value("kolbasa_producer_send_messages_failed", queue, mode))
        assertEquals(1L, observations("kolbasa_producer_send_duration_seconds", queue, mode))
        assertPositive(value("kolbasa_producer_send_bytes", queue, mode))

        // Queue gauges are read from the database: on the first call, then once a minute
        assertPositive(value("kolbasa_queue_size", queue))
    }

    @Test
    fun testReceiveAndDelete() {
        val queue = createQueue("metrics_receive")
        DatabaseProducer(dataSource).send(queue, listOf(SendMessage("first"), SendMessage("second")))

        val consumer = DatabaseConsumer(dataSource)
        val messages = consumer.receive(queue, limit = 2)
        consumer.delete(queue, messages)

        assertEquals(1.0, value("kolbasa_consumer_receive", queue))
        assertEquals(2.0, value("kolbasa_consumer_receive_messages", queue))
        assertEquals(0.0, value("kolbasa_consumer_receive_messages_unreadable", queue))
        assertEquals(1L, observations("kolbasa_consumer_receive_duration_seconds", queue))
        assertPositive(value("kolbasa_consumer_receive_bytes", queue))

        assertEquals(1.0, value("kolbasa_consumer_delete", queue))
        assertEquals(2.0, value("kolbasa_consumer_delete_messages", queue))
        assertEquals(1L, observations("kolbasa_consumer_delete_duration_seconds", queue))
    }

    @Test
    fun testMutate() {
        val queue = createQueue("metrics_mutate")
        val id = DatabaseProducer(dataSource).send(queue, "first").onlySuccessful().single().id

        DatabaseMutator(dataSource).addRemainingAttempts(queue, 1, id)

        val byId = "type" to "by_id"
        assertEquals(1.0, value("kolbasa_mutator_mutate", queue, byId))
        assertEquals(1.0, value("kolbasa_mutator_mutated_messages", queue, byId))
        assertEquals(1L, observations("kolbasa_mutator_mutate_duration_seconds", queue, byId))
    }

    @Test
    fun testSweep() {
        val queue = createQueue("metrics_sweep")

        // Nothing has expired, but the sweep itself is counted
        dataSource.inTransaction { connection -> SweepHelper.sweep(connection, queue, 100) }

        assertEquals(1.0, value("kolbasa_sweep", queue))
        assertEquals(0.0, value("kolbasa_sweep_removed_messages", queue))
        assertEquals(1L, observations("kolbasa_sweep_duration_seconds", queue))
    }

    @Test
    fun testChangeConfig() {
        val queue = createQueue("metrics_change_config")
        val producer = DatabaseProducer(dataSource)
        val mode = "partial_insert_type" to "UNTIL_FIRST_FAILURE"

        producer.send(queue, "first")

        // A new config over a new registry: the next call reports there, the old registry gets nothing more
        val newRegistry = PrometheusRegistry()
        Kolbasa.prometheusConfig = PrometheusConfig.Config(registry = newRegistry)
        producer.send(queue, "second")

        assertEquals(1.0, value("kolbasa_producer_send", queue, mode))
        assertEquals(1.0, value("kolbasa_producer_send", queue, mode, registry = newRegistry))
    }

    @Test
    fun testNone() {
        // Metrics are switched off
        Kolbasa.prometheusConfig = PrometheusConfig.None
        val queue = createQueue("metrics_none")

        DatabaseProducer(dataSource).send(queue, "first")

        // The registry has the metric, but nothing for this queue
        assertNull(value("kolbasa_producer_send", queue, "partial_insert_type" to "UNTIL_FIRST_FAILURE"))
    }

    // Every test uses its own queue name: the once-a-minute gauge refresh is remembered per queue name for the whole JVM
    private fun createQueue(name: String): Queue<String> {
        val queue = Queue.of(name, PredefinedDataTypes.String)
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
        return queue
    }

    /**
     * Value of a counter or a gauge for [queue], or `null` if the registry has no such value.
     */
    private fun value(
        name: String,
        queue: Queue<*>,
        vararg labels: Pair<String, String>,
        registry: PrometheusRegistry = this.registry
    ): Double? {
        return when (val point = dataPoint(registry, name, queue, *labels)) {
            null -> null
            is CounterDataPointSnapshot -> point.value
            is GaugeDataPointSnapshot -> point.value
            else -> error("$name is not a counter or a gauge")
        }
    }

    /**
     * How many values a histogram got for [queue], or `null` if the registry has no such histogram.
     */
    private fun observations(
        name: String,
        queue: Queue<*>,
        vararg labels: Pair<String, String>,
        registry: PrometheusRegistry = this.registry
    ): Long? {
        return when (val point = dataPoint(registry, name, queue, *labels)) {
            null -> null
            is DistributionDataPointSnapshot -> point.count
            else -> error("$name is not a histogram")
        }
    }

    private fun dataPoint(
        registry: PrometheusRegistry,
        name: String,
        queue: Queue<*>,
        vararg labels: Pair<String, String>
    ): DataPointSnapshot? {
        val metric = registry.scrape().find { it.metadata.name == name } ?: return null
        return metric.dataPoints.find { point ->
            point.labels.get("queue") == queue.name && labels.all { (label, value) -> point.labels.get(label) == value }
        }
    }

    private fun assertPositive(value: Double?) {
        assertTrue(value != null && value > 0) { "Expected a positive value, got $value" }
    }
}
