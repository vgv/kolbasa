package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.producer.ProducerOptions
import kolbasa.producer.ProducerSchemaHelpers
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.producer.datasource.ProducerInterceptor
import kolbasa.queue.Queue

class ClusterProducer<Data, Meta : Any>(
    private val cluster: Cluster,
    private val queue: Queue<Data, Meta>,
    private val producerOptions: ProducerOptions = ProducerOptions(),
    private val interceptors: List<ProducerInterceptor<Data, Meta>> = emptyList(),
) : Producer<Data, Meta> {

    override fun send(request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        request.effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(
            producerOptions = producerOptions,
            sendOptions = request.sendOptions,
            shardStrategy = Kolbasa.shardStrategy
        )

        val currentState = cluster.getState()
        val producer = currentState.getProducer(this, request.effectiveShard) { dataSource ->
            DatabaseProducer(dataSource, queue, producerOptions, interceptors)
        }

        return producer.send(request)
    }
}
