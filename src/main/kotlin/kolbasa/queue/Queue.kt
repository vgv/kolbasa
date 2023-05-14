package kolbasa.queue

import kolbasa.queue.meta.MetaClass
import kolbasa.schema.Const

data class Queue<V, M : Any> @JvmOverloads constructor(
    val name: String,
    val dataType: QueueDataType<V>,
    val options: QueueOptions? = null,
    val metadata: Class<M>? = null
) {

    init {
        Checks.checkQueueName(name)
    }

    internal val dbTableName = Const.QUEUE_TABLE_NAME_PREFIX + name

    internal val metadataDescription: MetaClass<M>? = metadata?.let { MetaClass.of(metadata) }

}

