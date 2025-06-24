package kolbasa.queue

import kolbasa.Kolbasa
import kolbasa.queue.meta.MetaClass
import kolbasa.schema.Const
import kolbasa.stats.opentelemetry.EmptyQueueTracing
import kolbasa.stats.opentelemetry.OpenTelemetryConfig
import kolbasa.stats.opentelemetry.OpenTelemetryQueueTracing
import kolbasa.stats.opentelemetry.QueueTracing
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.EmptyQueueMetrics
import kolbasa.stats.prometheus.metrics.PrometheusQueueMetrics
import kolbasa.stats.prometheus.metrics.QueueMetrics

data class Queue<Data, Meta : Any> @JvmOverloads constructor(
    /**
     * Queue name. Must be unique.
     *
     * There is a direct mapping between queue name and database table name. Table name is prefixed with `q_`, e.g.
     * queue with name `customer_mail` will have table name `q_customer_mail`.
     */
    val name: String,

    /**
     * Database data type for queue messages (jsonb, bytea, int, bigint etc.).
     *
     * If you want to explore/change your queue externally or put messages directly from database trigger (for example),
     * choose datatype in accordance with your needs.
     *
     * If you don't want to do anything "strange" with your queue except sending/receiving messages through Kolbasa,
     * it's safe to choose [PredefinedDataTypes.ByteArray] since it's the most data and serialization agnostic format.
     */
    val databaseDataType: DatabaseQueueDataType<Data>,

    /**
     * Global queue options.
     *
     * Producers, consumers, send request and even particular message options can override global queue options. Read more
     * details in [QueueOptions]
     */
    val options: QueueOptions? = null,

    /**
     * Metadata class for queue.
     *
     * Metadata is a set of fields that will be stored in the database along with the message. Each message has
     * its own metadata values. Metadata is not required and can be null.
     * It's useful if you want to filter messages by some fields or sort by them.
     *
     * Now Kolbasa supports only Kotlin data classes, Java Record classes and Java Beans as metadata class.
     */
    val metadata: Class<Meta>? = null
) {

    init {
        Checks.checkQueueName(name)
    }

    internal val dbTableName = Const.QUEUE_TABLE_NAME_PREFIX + name

    internal val metadataDescription: MetaClass<Meta>? = metadata?.let { MetaClass.of(metadata) }

    internal val queueMetrics: QueueMetrics by lazy {
        when (val config = Kolbasa.prometheusConfig) {
            // No Prometheus config - no metrics collection
            is PrometheusConfig.None -> EmptyQueueMetrics

            // Performance optimization: create all prometheus metrics with correct labels (queue name etc.)
            // and cache it in the queue object to avoid excessive allocations.
            // Recommendation: https://prometheus.github.io/client_java/getting-started/performance/
            is PrometheusConfig.Config -> PrometheusQueueMetrics(name, config)
        }
    }

    internal val queueTracing: QueueTracing<Data, Meta> by lazy {
        when (val config = Kolbasa.openTelemetryConfig) {
            // No OpenTelemetry config - no OT data collection/propagation
            is OpenTelemetryConfig.None -> EmptyQueueTracing()

            // Performance optimization: create all opentelemetry stuff for the queue (instrumenter, setters etc.)
            // and cache it to avoid excessive allocations.
            is OpenTelemetryConfig.Config -> OpenTelemetryQueueTracing(name, config)
        }
    }

    class Builder<Data> internal constructor(
        private val name: String,
        private val databaseDataType: DatabaseQueueDataType<Data>,
    ) {
        private var options: QueueOptions? = null

        fun options(options: QueueOptions) = apply { this.options = options }
        fun <Meta : Any> metadata(metadata: Class<Meta>): BuilderWithMeta<Data, Meta> {
            // switch to another builder
            val builderWithMeta = BuilderWithMeta<Data, Meta>(name, databaseDataType)
            builderWithMeta.metadata(metadata)
            this.options?.let { currentOptions ->
                builderWithMeta.options(currentOptions)
            }
            return builderWithMeta
        }

        fun build(): Queue<Data, Unit> = Queue(name, databaseDataType, options, metadata = null)
    }

    class BuilderWithMeta<Data, Meta : Any> internal constructor(
        private val name: String,
        private val databaseDataType: DatabaseQueueDataType<Data>,
    ) {
        private var options: QueueOptions? = null
        private var metadata: Class<Meta>? = null

        fun options(options: QueueOptions) = apply { this.options = options }
        fun metadata(metadata: Class<Meta>) = apply { this.metadata = metadata }

        fun build(): Queue<Data, Meta> = Queue(name, databaseDataType, options, metadata)
    }

    companion object {
        @JvmStatic
        fun <Data> builder(name: String, databaseDataType: DatabaseQueueDataType<Data>) = Builder(name, databaseDataType)

        /**
         * Creates a new queue with the given name and database data type.
         */
        @JvmStatic
        fun <Data> of(name: String, databaseDataType: DatabaseQueueDataType<Data>): Queue<Data, Unit> {
            return builder(name, databaseDataType).build()
        }

        /**
         * Creates a new queue with the given name, database data type and metadata class.
         */
        @JvmStatic
        fun <Data, Meta : Any> of(
            name: String,
            databaseDataType: DatabaseQueueDataType<Data>,
            metadata: Class<Meta>
        ): Queue<Data, Meta> {
            return builder(name, databaseDataType)
                .metadata(metadata)
                .build()
        }
    }
}

