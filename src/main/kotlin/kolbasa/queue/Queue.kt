package kolbasa.queue

import kolbasa.queue.meta.MetaClass
import kolbasa.schema.Const
import kolbasa.stats.prometheus.QueueMetrics

data class Queue<Data, Meta : Any> @JvmOverloads constructor(
    val name: String,
    val dataType: QueueDataType<Data>,
    val options: QueueOptions? = null,
    val metadata: Class<Meta>? = null
) {

    init {
        Checks.checkQueueName(name)
    }

    internal val dbTableName = Const.QUEUE_TABLE_NAME_PREFIX + name

    internal val metadataDescription: MetaClass<Meta>? = metadata?.let { MetaClass.of(metadata) }

    internal val queueMetrics by lazy { QueueMetrics(name) }
}

