package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.producer.ProducerOptions
import kolbasa.producer.ProducerSchemaHelpers
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.queue.Queue

class ClusterProducer<Data, Meta : Any>(
    private val cluster: Cluster,
    private val queue: Queue<Data, Meta>,
    private val producerOptions: ProducerOptions = ProducerOptions()
) : Producer<Data, Meta> {

    override fun <D, M : Any> send(queue: Queue<D, M>, request: SendRequest<D, M>): SendResult<D, M> {
        request.effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(
            producerOptions = producerOptions,
            sendOptions = request.sendOptions,
            shardStrategy = Kolbasa.shardStrategy
        )

        val currentState = cluster.getState()
        val producer = currentState.getProducer(this, request.effectiveShard) { dataSource ->
            DatabaseProducer(dataSource, queue, producerOptions)
        }

        return producer.send(queue, request)
    }

    override fun send(request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        return send(queue, request)
    }
}
