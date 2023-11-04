package kolbasa.cluster.simple

import kolbasa.producer.Producer
import kolbasa.queue.Queue

interface ProducerProvider {

    fun <Data, Meta : Any> producer(queue: Queue<Data, Meta>): Producer<Data, Meta>

}
