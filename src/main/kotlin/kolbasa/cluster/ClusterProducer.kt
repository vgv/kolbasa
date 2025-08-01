package kolbasa.cluster

import kolbasa.Kolbasa
import kolbasa.producer.ProducerOptions
import kolbasa.producer.ProducerSchemaHelpers
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.queue.Queue
import java.util.concurrent.CompletableFuture

class ClusterProducer(
    private val cluster: Cluster,
    private val producerOptions: ProducerOptions = ProducerOptions()
) : Producer {

    override fun <Data, Meta : Any> send(queue: Queue<Data, Meta>, request: SendRequest<Data, Meta>): SendResult<Data, Meta> {
        request.effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(
            sendOptions = request.sendOptions,
            producerOptions = producerOptions,
            shardStrategy = Kolbasa.shardStrategy
        )

        val currentState = cluster.getState()
        val producer = currentState.getProducer(this, request.effectiveShard) { nodeId, dataSource ->
            val p = ConnectionAwareDatabaseProducer(nodeId, producerOptions)
            DatabaseProducer(dataSource, p)
        }

        return producer.send(queue, request)
    }

    override fun <Data, Meta : Any> sendAsync(
        queue: Queue<Data, Meta>,
        request: SendRequest<Data, Meta>
    ): CompletableFuture<SendResult<Data, Meta>> {
        // TODO: make it smarter
        val executor = ProducerSchemaHelpers.calculateAsyncExecutor(
            customExecutor = producerOptions.asyncExecutor,
            defaultExecutor = Kolbasa.asyncExecutor
        )

        return CompletableFuture.supplyAsync({ send(queue, request) }, executor)
    }
}
