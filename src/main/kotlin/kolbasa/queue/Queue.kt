package kolbasa.queue

import kolbasa.Kolbasa
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.Metadata
import kolbasa.schema.Const
import kolbasa.stats.opentelemetry.EmptyQueueTracing
import kolbasa.stats.opentelemetry.OpenTelemetryConfig
import kolbasa.stats.opentelemetry.OpenTelemetryQueueTracing
import kolbasa.stats.opentelemetry.QueueTracing
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.EmptyQueueMetrics
import kolbasa.stats.prometheus.PrometheusQueueMetrics
import kolbasa.stats.prometheus.QueueMetrics
import java.time.Duration

/**
 * A queue: its name, the type of its messages, its metadata and its options.
 *
 * A [Queue] object only describes a queue table, it is not the table itself. Creating one touches no database.
 * Call [SchemaHelpers.createOrUpdateQueues][kolbasa.schema.SchemaHelpers.createOrUpdateQueues] to create or
 * update the table. The name maps directly to the table name: a queue named `orders` lives in table `q_orders`.
 *
 * A queue is a description, not a value, so there is no point in creating it more than once. Create it once per
 * application and reuse it, the same way you declare a [MetaField][kolbasa.queue.meta.MetaField].
 *
 * ## Usage Example
 *
 * ```kotlin
 * val orders = Queue.of("orders", PredefinedDataTypes.String, Metadata.of(ACCOUNT_ID))
 * val customers = Queue.of("customers", PredefinedDataTypes.String)
 *
 * // Both tables are created, or brought up to date, in one call
 * SchemaHelpers.createOrUpdateQueues(dataSource, orders, customers)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var orders = Queue.of("orders", PredefinedDataTypes.String, Metadata.of(ACCOUNT_ID));
 * var customers = Queue.of("customers", PredefinedDataTypes.String);
 *
 * // Both tables are created, or brought up to date, in one call
 * SchemaHelpers.createOrUpdateQueues(dataSource, orders, customers);
 * ```
 */
data class Queue<Data> internal constructor(
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
    val options: QueueOptions,

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
    val metadata: Metadata,

    /**
     * The role of this queue: [MAIN][QueueRole.MAIN], [DLQ][QueueRole.DLQ], or [ARCHIVE][QueueRole.ARCHIVE].
     *
     * Users can only create [MAIN][QueueRole.MAIN] queues directly. [DLQ][QueueRole.DLQ] and
     * [ARCHIVE][QueueRole.ARCHIVE] queues are created automatically when the corresponding options
     * are enabled in [QueueOptions].
     */
    val queueRole: QueueRole
) {

    /**
     * Creates a [MAIN][QueueRole.MAIN] queue, the only kind that can be created directly.
     *
     * DLQ and archive queues are not created this way. Enable them in [QueueOptions] and read them back from
     * [deadLetterQueue] and [archiveQueue].
     */
    // Public constructor — users can only create MAIN queues
    @JvmOverloads
    constructor(
        name: String,
        databaseDataType: DatabaseQueueDataType<Data>,
        options: QueueOptions = QueueOptions.DEFAULT,
        metadata: Metadata = Metadata.EMPTY
    ) : this(name, databaseDataType, options, metadata, QueueRole.MAIN)

    init {
        Checks.checkQueueName(name, queueRole)
        Checks.checkQueueRole(queueRole, options)
    }

    internal val dbTableName = QueueHelpers.generateQueueDbName(name)

    /**
     * Returns the Dead Letter Queue for this queue, or null if DLQ feature is not enabled.
     * The returned Queue can be used with Consumer, Inspector, Mutator, etc.
     */
    val deadLetterQueue: Queue<Data>? = if (queueRole == QueueRole.MAIN && options.dlqOptions != null)
        createCompanionQueue(this, QueueRole.DLQ, Const.DLQ_TABLE_NAME_SUFFIX)
    else
        null

    /**
     * Returns the Archive queue for this queue, or null if Archive feature is not enabled.
     * The returned Queue can be used with Consumer, Inspector, Mutator, etc.
     */
    val archiveQueue: Queue<Data>? = if (queueRole == QueueRole.MAIN && options.archiveQueueOptions != null)
        createCompanionQueue(this, QueueRole.ARCHIVE, Const.ARCHIVE_TABLE_NAME_SUFFIX)
    else
        null

    internal val queueMetrics: QueueMetrics by lazy {
        when (val config = Kolbasa.prometheusConfig) {
            // No Prometheus config - no metrics collection
            is PrometheusConfig.None -> EmptyQueueMetrics

            // Performance optimization: create all prometheus metrics with correct labels (queue name etc.)
            // and cache it in the queue object to avoid excessive allocations.
            // Recommendation: https://prometheus.github.io/client_java/getting-started/performance/
            is PrometheusConfig.Config -> PrometheusQueueMetrics(this, config)
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

    /** Builder for flexible [Queue] creation, when only some of the properties need to be set. */
    class Builder<Data> internal constructor(
        private val name: String,
        private val databaseDataType: DatabaseQueueDataType<Data>,
    ) {
        private var options: QueueOptions = QueueOptions.DEFAULT
        private var metadata: Metadata = Metadata.EMPTY

        /** Sets [Queue.options] – delays, attempts, visibility timeout, DLQ, archive and the SQL put function. */
        fun options(options: QueueOptions) = apply { this.options = options }

        /** Sets [Queue.metadata] – the meta fields the messages of this queue may carry. */
        fun metadata(metadata: Metadata) = apply { this.metadata = metadata }

        /** Creates a new [Queue] instance: a fresh one on every call, nothing is cached or reused. */
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

        // Creates a companion queue for the given MAIN queue.
        // Right now only two companion queue roles exist: DLQ and ARCHIVE.
        private fun <Data> createCompanionQueue(
            mainQueue: Queue<Data>,
            role: QueueRole,
            queueNameSuffix: String
        ): Queue<Data> {
            // Main queue metadata with indexes stripped (no unique indexes on companions)
            val strippedParentFields = mainQueue.metadata.fields.map { it.withOption(FieldOption.NONE) }

            // Add predefined original-value meta fields
            val companionFields = when (role) {
                QueueRole.DLQ -> Metadata.DLQ_FIELDS
                QueueRole.ARCHIVE -> Metadata.ARCHIVE_FIELDS
                QueueRole.MAIN -> error("MAIN queue cannot be a companion queue: ${mainQueue.name}")
            }
            val companionMetadata = Metadata(strippedParentFields + companionFields)

            // Companion queues have:
            //   - No delay (messages are already "ready")
            //   - High attempts (we don't want DLQ/Archive messages to expire)
            //   - No DLQ/Archive of their own (prevent recursion)
            val companionOptions = QueueOptions(
                defaultDelay = Duration.ZERO,
                defaultAttempts = Int.MAX_VALUE,
                defaultVisibilityTimeout = mainQueue.options.defaultVisibilityTimeout,
                dlqOptions = null,
                archiveQueueOptions = null,
                sqlPutFunction = false
            )

            return Queue(
                name = mainQueue.name + queueNameSuffix,
                databaseDataType = mainQueue.databaseDataType,
                options = companionOptions,
                metadata = companionMetadata,
                queueRole = role
            )
        }
    }
}
