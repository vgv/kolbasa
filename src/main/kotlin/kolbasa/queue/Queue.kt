package kolbasa.queue

import kolbasa.queue.meta.MetaClass
import kolbasa.schema.Const
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

    // Performance optimization: create all prometheus metrics with correct labels (queue name etc.)
    // and cache it in the queue object to avoid excessive allocations.
    // Recommendation: https://prometheus.github.io/client_java/getting-started/performance/
    internal val queueMetrics by lazy { QueueMetrics(name) }
}

