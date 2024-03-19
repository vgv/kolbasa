package kolbasa.cluster.simple

import kolbasa.consumer.Consumer
import kolbasa.queue.Queue

interface ConsumerProvider {

    fun <Data, Meta : Any> consumer(queue: Queue<Data, Meta>): Consumer<Data, Meta>

    fun <Data, Meta : Any> consumer(queue: Queue<Data, Meta>, shard: Int): Consumer<Data, Meta>

}
