package kolbasa.telemetry.metrics

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.search.Search
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import kolbasa.Kolbasa
import kolbasa.consumer.ConsumerOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.consumer.sweep.SweepHelper
import kolbasa.mutator.MutatorOptions
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.producer.ProducerOptions
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

// Kolbasa.metricsConfig is global, so no other test may run at the same time
@Isolated
class MetricsTest : AbstractPostgreSQLTest() {

    // A new registry for every test, so a test sees only its own metrics
    private val registry = SimpleMeterRegistry()

    @BeforeEach
    fun setMetricsConfig() {
        Kolbasa.metricsConfig = MetricsConfig.Config(registry)
    }

    @AfterEach
    fun resetMetricsConfig() {
        Kolbasa.metricsConfig = MetricsConfig.None
    }

    @Test
    fun testSend() {
        val queue = createQueue("metrics_send")

        DatabaseProducer(dataSource).send(queue, listOf(SendMessage("first"), SendMessage("second")))

        // The partial insert mode is a tag too, this is the default one
        val mode = "partial_insert_type" to "UNTIL_FIRST_FAILURE"
        assertEquals(1.0, value("kolbasa.producer.send", queue, mode))
        assertEquals(2.0, value("kolbasa.producer.send.messages", queue, mode))
        assertEquals(0.0, value("kolbasa.producer.send.messages.failed", queue, mode))
        assertEquals(1L, observations("kolbasa.producer.send.duration", queue, mode))
        assertPositive(value("kolbasa.producer.send.bytes", queue, mode))

        // Queue gauges are read from the database: on the first call, then once a minute
        assertPositive(value("kolbasa.queue.size", queue))
    }

    @Test
    fun testReceiveAndDelete() {
        val queue = createQueue("metrics_receive")
        DatabaseProducer(dataSource).send(queue, listOf(SendMessage("first"), SendMessage("second")))

        val consumer = DatabaseConsumer(dataSource)
        val messages = consumer.receive(queue, limit = 2)
        consumer.delete(queue, messages)

        assertEquals(1.0, value("kolbasa.consumer.receive", queue))
        assertEquals(2.0, value("kolbasa.consumer.receive.messages", queue))
        assertEquals(0.0, value("kolbasa.consumer.receive.messages.unreadable", queue))
        assertEquals(1L, observations("kolbasa.consumer.receive.duration", queue))
        assertPositive(value("kolbasa.consumer.receive.bytes", queue))

        assertEquals(1.0, value("kolbasa.consumer.delete", queue))
        assertEquals(2.0, value("kolbasa.consumer.delete.messages", queue))
        assertEquals(1L, observations("kolbasa.consumer.delete.duration", queue))
    }

    @Test
    fun testBytes() {
        val queue = createQueue("metrics_bytes")
        val mode = "partial_insert_type" to "UNTIL_FIRST_FAILURE"

        // 6 characters, 12 bytes in UTF-8
        DatabaseProducer(dataSource).send(queue, "Привет")
        DatabaseConsumer(dataSource).receive(queue)

        assertEquals(12.0, value("kolbasa.producer.send.bytes", queue, mode))
        assertEquals(12.0, value("kolbasa.consumer.receive.bytes", queue))
    }

    @Test
    fun testMutate() {
        val queue = createQueue("metrics_mutate")
        val id = DatabaseProducer(dataSource).send(queue, "first").onlySuccessful().single().id

        DatabaseMutator(dataSource).addRemainingAttempts(queue, 1, id)

        val byId = "type" to "by_id"
        assertEquals(1.0, value("kolbasa.mutator.mutate", queue, byId))
        assertEquals(1.0, value("kolbasa.mutator.mutated.messages", queue, byId))
        assertEquals(1L, observations("kolbasa.mutator.mutate.duration", queue, byId))
    }

    @Test
    fun testSweep() {
        val queue = createQueue("metrics_sweep")

        // Nothing has expired, but the sweep itself is counted
        dataSource.inTransaction { connection -> SweepHelper.sweep(connection, queue, 100) }

        assertEquals(1.0, value("kolbasa.sweep", queue))
        assertEquals(0.0, value("kolbasa.sweep.removed.messages", queue))
        assertEquals(1L, observations("kolbasa.sweep.duration", queue))
    }

    @Test
    fun testSweep_OwnConfig() {
        val queue = createQueue("metrics_sweep_own_config")

        // A sweep by hand, with its own config over its own registry
        val ownRegistry = SimpleMeterRegistry()
        val config = MetricsConfig.Config(ownRegistry)
        dataSource.inTransaction { connection -> SweepHelper.sweep(connection, queue, 100, config) }

        // Its own registry gets the sweep, the global one gets nothing
        assertEquals(1.0, value("kolbasa.sweep", queue, registry = ownRegistry))
        assertNull(value("kolbasa.sweep", queue))
    }

    @Test
    fun testChangeConfig() {
        val queue = createQueue("metrics_change_config")
        val producer = DatabaseProducer(dataSource)
        val mode = "partial_insert_type" to "UNTIL_FIRST_FAILURE"

        producer.send(queue, "first")

        // A new config over a new registry: the next call reports there, the old registry gets nothing more
        val newRegistry = SimpleMeterRegistry()
        Kolbasa.metricsConfig = MetricsConfig.Config(newRegistry)
        producer.send(queue, "second")

        assertEquals(1.0, value("kolbasa.producer.send", queue, mode))
        assertEquals(1.0, value("kolbasa.producer.send", queue, mode, registry = newRegistry))

        // Each config reads the queue gauges on its own first call, so both registries have them
        assertPositive(value("kolbasa.queue.size", queue))
        assertPositive(value("kolbasa.queue.size", queue, registry = newRegistry))
    }

    @Test
    fun testTwoConfigsOneRegistry() {
        val queue = createQueue("metrics_two_configs")
        val mode = "partial_insert_type" to "UNTIL_FIRST_FAILURE"

        // A second config over the same registry as the global one: it shares the metrics, it doesn't fail
        val config = MetricsConfig.Config(registry)
        DatabaseProducer(dataSource).send(queue, "first")
        DatabaseProducer(dataSource, ProducerOptions(metricsConfig = config)).send(queue, "second")

        // Both calls went to the same counter
        assertEquals(2.0, value("kolbasa.producer.send", queue, mode))
    }

    @Test
    fun testTwoConfigsOneRegistry_GaugesReadOnce() {
        val queue = createQueue("metrics_two_configs_gauges")

        // The first call reads the queue gauges from the database
        DatabaseProducer(dataSource).send(queue, "first")
        val size = value("kolbasa.queue.size", queue)
        assertPositive(size)

        // A second config over the same registry, and the queue grows a lot. The gauges were read less than
        // a minute ago, so the second config doesn't read them again
        val options = ProducerOptions(metricsConfig = MetricsConfig.Config(registry))
        DatabaseProducer(dataSource, options).send(queue, (1..1000).map { SendMessage("message $it") })

        assertEquals(size, value("kolbasa.queue.size", queue))
    }

    @Test
    fun testNone() {
        // Metrics are switched off
        Kolbasa.metricsConfig = MetricsConfig.None
        val queue = createQueue("metrics_none")

        DatabaseProducer(dataSource).send(queue, "first")

        // The registry has nothing for this queue
        assertNull(value("kolbasa.producer.send", queue, "partial_insert_type" to "UNTIL_FIRST_FAILURE"))
    }

    @Test
    fun testProducerOptions() {
        val queue = createQueue("metrics_producer_options")
        val mode = "partial_insert_type" to "UNTIL_FIRST_FAILURE"

        // The producer has its own config over its own registry
        val ownRegistry = SimpleMeterRegistry()
        val options = ProducerOptions(metricsConfig = MetricsConfig.Config(ownRegistry))
        DatabaseProducer(dataSource, options).send(queue, "first")

        // Its own registry gets the call, the global one gets nothing
        assertEquals(1.0, value("kolbasa.producer.send", queue, mode, registry = ownRegistry))
        assertNull(value("kolbasa.producer.send", queue, mode))
    }

    @Test
    fun testProducerOptions_None() {
        val queue = createQueue("metrics_producer_none")

        // The global config is on, but this producer switches the metrics off
        val options = ProducerOptions(metricsConfig = MetricsConfig.None)
        DatabaseProducer(dataSource, options).send(queue, "first")

        assertNull(value("kolbasa.producer.send", queue, "partial_insert_type" to "UNTIL_FIRST_FAILURE"))
    }

    @Test
    fun testConsumerOptions() {
        val queue = createQueue("metrics_consumer_options")
        DatabaseProducer(dataSource).send(queue, "first")

        // The consumer has its own config over its own registry
        val ownRegistry = SimpleMeterRegistry()
        val options = ConsumerOptions(metricsConfig = MetricsConfig.Config(ownRegistry))
        val consumer = DatabaseConsumer(dataSource, options)

        // A sweep on every receive: it reports to the consumer's config too
        val sweepConfig = Kolbasa.sweepConfig
        Kolbasa.sweepConfig = SweepConfig(probability = SweepConfig.SWEEP_IS_ALWAYS_ON)
        try {
            consumer.delete(queue, consumer.receive(queue, limit = 1))
        } finally {
            Kolbasa.sweepConfig = sweepConfig
        }

        // Its own registry gets the calls, the global one gets nothing
        assertEquals(1.0, value("kolbasa.consumer.receive", queue, registry = ownRegistry))
        assertEquals(1.0, value("kolbasa.consumer.delete", queue, registry = ownRegistry))
        assertEquals(1.0, value("kolbasa.sweep", queue, registry = ownRegistry))
        assertNull(value("kolbasa.consumer.receive", queue))
        assertNull(value("kolbasa.consumer.delete", queue))
        assertNull(value("kolbasa.sweep", queue))
    }

    @Test
    fun testMutatorOptions() {
        val queue = createQueue("metrics_mutator_options")
        val id = DatabaseProducer(dataSource).send(queue, "first").onlySuccessful().single().id

        // The mutator has its own config over its own registry
        val ownRegistry = SimpleMeterRegistry()
        val options = MutatorOptions(metricsConfig = MetricsConfig.Config(ownRegistry))
        DatabaseMutator(dataSource, options).addRemainingAttempts(queue, 1, id)

        // Its own registry gets the call, the global one gets nothing
        val byId = "type" to "by_id"
        assertEquals(1.0, value("kolbasa.mutator.mutate", queue, byId, registry = ownRegistry))
        assertNull(value("kolbasa.mutator.mutate", queue, byId))
    }

    @Test
    fun testPrometheusNames() {
        val queue = createQueue("metrics_prometheus_names")

        // Every kind of call, each reporting to a Prometheus registry
        val prometheus = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val config = MetricsConfig.Config(prometheus)
        val id = DatabaseProducer(dataSource, ProducerOptions(metricsConfig = config))
            .send(queue, "first").onlySuccessful().single().id
        DatabaseMutator(dataSource, MutatorOptions(metricsConfig = config)).addRemainingAttempts(queue, 1, id)
        val consumer = DatabaseConsumer(dataSource, ConsumerOptions(metricsConfig = config))
        consumer.delete(queue, consumer.receive(queue, limit = 1))
        dataSource.inTransaction { connection -> SweepHelper.sweep(connection, queue, 100, config) }

        // The names Prometheus sees: dashboards and alerts are built on them, so renaming one breaks them
        val gauges = listOf("kolbasa_queue_messages", "kolbasa_queue_message_age_seconds", "kolbasa_queue_size_bytes")
        val counters = listOf(
            "kolbasa_producer_send_total",
            "kolbasa_producer_send_messages_total",
            "kolbasa_producer_send_messages_failed_total",
            "kolbasa_producer_send_bytes_total",
            "kolbasa_consumer_receive_total",
            "kolbasa_consumer_receive_messages_total",
            "kolbasa_consumer_receive_messages_unreadable_total",
            "kolbasa_consumer_receive_bytes_total",
            "kolbasa_consumer_delete_total",
            "kolbasa_consumer_delete_messages_total",
            "kolbasa_sweep_total",
            "kolbasa_sweep_removed_messages_total",
            "kolbasa_mutator_mutate_total",
            "kolbasa_mutator_mutate_iterations_total",
            "kolbasa_mutator_mutated_messages_total"
        )
        val histograms = listOf(
            "kolbasa_producer_send_duration_seconds",
            "kolbasa_consumer_receive_duration_seconds",
            "kolbasa_consumer_delete_duration_seconds",
            "kolbasa_sweep_duration_seconds",
            "kolbasa_mutator_mutate_duration_seconds"
        ).flatMap { name -> listOf("_bucket", "_count", "_sum", "_max").map { name + it } }

        // Sample names: what comes before the labels on every line that is not a comment
        val scraped = prometheus.scrape().lines()
            .filter { it.startsWith("kolbasa_") }
            .map { it.substringBefore('{').substringBefore(' ') }
            .toSet()

        assertEquals((gauges + counters + histograms).toSet(), scraped)
    }

    private fun createQueue(name: String): Queue<String> {
        val queue = Queue.of(name, PredefinedDataTypes.String)
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
        return queue
    }

    /**
     * Value of a counter or a gauge for [queue], or `null` if the registry has no such meter.
     */
    private fun value(
        name: String,
        queue: Queue<*>,
        vararg tags: Pair<String, String>,
        registry: MeterRegistry = this.registry
    ): Double? {
        val search = search(registry, name, queue, *tags)
        search.counter()?.let { return it.count() }
        search.gauge()?.let { return it.value() }
        return null
    }

    /**
     * How many values a timer got for [queue], or `null` if the registry has no such timer.
     */
    private fun observations(
        name: String,
        queue: Queue<*>,
        vararg tags: Pair<String, String>,
        registry: MeterRegistry = this.registry
    ): Long? {
        return search(registry, name, queue, *tags).timer()?.count()
    }

    private fun search(
        registry: MeterRegistry,
        name: String,
        queue: Queue<*>,
        vararg tags: Pair<String, String>
    ): Search {
        var search = registry.find(name).tag("queue", queue.name)
        tags.forEach { (key, value) -> search = search.tag(key, value) }
        return search
    }

    private fun assertPositive(value: Double?) {
        assertTrue(value != null && value > 0) { "Expected a positive value, got $value" }
    }
}
