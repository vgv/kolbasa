package kolbasa.cluster.simple

import kolbasa.consumer.Consumer

interface ConsumerProvider<Data, Meta : Any> {
    fun consumer(): Consumer<Data, Meta>
}
