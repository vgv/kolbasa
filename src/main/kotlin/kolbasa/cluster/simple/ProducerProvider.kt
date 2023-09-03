package kolbasa.cluster.simple

import kolbasa.producer.Producer

interface ProducerProvider<Data, Meta : Any> {
    fun producer(): Producer<Data, Meta>
}
