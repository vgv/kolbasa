package kolbasa.queue

import kolbasa.Kolbasa
import kolbasa.queue.meta.Metadata
import kolbasa.stats.opentelemetry.EmptyQueueTracing
import kolbasa.stats.opentelemetry.OpenTelemetryConfig
import kolbasa.stats.opentelemetry.OpenTelemetryQueueTracing
import kolbasa.stats.opentelemetry.QueueTracing
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.EmptyQueueMetrics
import kolbasa.stats.prometheus.metrics.PrometheusQueueMetrics
import kolbasa.stats.prometheus.metrics.QueueMetrics

data class Queue<Data> @JvmOverloads constructor(
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
     * Queue options.
     *
     * Producers, consumers, send request and even particular message options can override queue options. Read more
     * details in [QueueOptions]
     */
    val options: QueueOptions = QueueOptions.DEFAULT,

    /**
     * Metadata for a queue.
     *
     * Metadata is a set of fields that will be stored in the database along with the message. Each message has
     * its own metadata values. Metadata is not required and can be empty.
     * It's useful if you want to filter messages by some fields or sort by them.
     *
     * It may not be entirely obvious at first whether you should store a specific data in the message body or in metadata.
     * A few simple questions can help clarify this. Let's use the abstract field `account_id int` as an example. Should this
     * field be part of the message body along with the rest of the data or stored separately as metadata?
     *
     * 1. Do you plan to filter or sort by this field? If the answer is "Yes," it is metadata.
     * 2. Have you built complex, custom message routing between different clusters/queues based on filtering by this field?
     * Not necessarily using Kolbasa alone, perhaps involving third-party DBMS utilities for replication/monitoring? If the
     * answer is "Yes," there's a 99% chance this field should be metadata.
     * 3. Have you implemented monitoring/alerting based on this field using external utils (psql, Zabbix, Nagios)?
     * For example, alerts for high message volume for a specific `account_id`? If the answer is "Yes", it's very likely this
     * field is metadata.
     * 4. Is this field part of business logic? Is it involved in any business code? If the answer is "Yes," it is the message body.
     * 5. For everything to work, do you need to pass not only the message body but also this field from the metadata to the
     * business code?  If the answer is "Yes, this is 99% likely a mistake, and in this case, this field should be part of the
     * message body.
     * 6. If you disable metadata retrieval, will the business logic break? If the answer is "Yes," you've placed the business
     * data in the wrong place (metadata); it should be part of the message body.
     */
    val metadata: Metadata = Metadata.EMPTY
) {

    init {
        Checks.checkQueueName(name)
    }

    internal val dbTableName = QueueHelpers.generateQueueDbName(name)

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

    internal val queueTracing: QueueTracing<Data> by lazy {
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
        private var options: QueueOptions = QueueOptions.DEFAULT
        private var metadata: Metadata = Metadata.EMPTY

        fun options(options: QueueOptions) = apply { this.options = options }
        fun metadata(metadata: Metadata) = apply { this.metadata = metadata }

        fun build(): Queue<Data> = Queue(name, databaseDataType, options, metadata)
    }

    companion object {
        @JvmStatic
        fun <Data> builder(name: String, databaseDataType: DatabaseQueueDataType<Data>) = Builder(name, databaseDataType)

        /**
         * Creates a new queue with the given name and database data type.
         */
        @JvmStatic
        fun <Data> of(name: String, databaseDataType: DatabaseQueueDataType<Data>): Queue<Data> {
            return builder(name, databaseDataType).build()
        }

        /**
         * Creates a new queue with the given name, database data type and metadata class.
         */
        @JvmStatic
        fun <Data> of(
            name: String,
            databaseDataType: DatabaseQueueDataType<Data>,
            metadata: Metadata
        ): Queue<Data> {
            return builder(name, databaseDataType)
                .metadata(metadata)
                .build()
        }
    }
}

