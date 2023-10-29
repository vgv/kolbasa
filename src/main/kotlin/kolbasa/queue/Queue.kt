package kolbasa.queue

import kolbasa.queue.meta.MetaClass
import kolbasa.schema.Const
import kolbasa.stats.prometheus.QueueMetrics

data class Queue<Data, Meta : Any> @JvmOverloads constructor(
    val name: String,
    val databaseDataType: DatabaseQueueDataType<Data>,
    val options: QueueOptions? = null,
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

