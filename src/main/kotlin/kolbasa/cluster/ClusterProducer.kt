package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.producer.ProducerOptions
import kolbasa.producer.ProducerSchemaHelpers
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.queue.Queue

class ClusterProducer(
    private val cluster: Cluster,
    private val producerOptions: ProducerOptions = ProducerOptions()
) : Producer {

    override fun <Data, Meta : Any> send(queue: Queue<Data, Meta>, request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        request.effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(
            producerOptions = producerOptions,
            sendOptions = request.sendOptions,
            shardStrategy = Kolbasa.shardStrategy
        )

        val currentState = cluster.getState()
        val producer = currentState.getProducer(this, request.effectiveShard) { dataSource ->
            DatabaseProducer(dataSource, producerOptions)
        }

        return producer.send(queue, request)
    }

}
