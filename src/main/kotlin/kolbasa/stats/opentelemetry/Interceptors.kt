package kolbasa.stats.opentelemetry

import kolbasa.Kolbasa
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.connection.ConnectionAwareConsumerInterceptor
import kolbasa.consumer.datasource.ConsumerInterceptor
import kolbasa.producer.SendRequest
import kolbasa.producer.SendResult
import kolbasa.producer.datasource.ProducerInterceptor
import kolbasa.queue.Queue
import java.sql.Connection

class TracingProducerInterceptor<Data, Meta : Any>(
    private val queue: Queue<Data, Meta>
) : ProducerInterceptor<Data, Meta> {

    override fun aroundSend(
        request: SendRequest<Data, Meta>,
        call: (SendRequest<Data, Meta>) -> SendResult<Data, Meta>
    ): SendResult<Data, Meta> {
        return queue.queueTracing.makeProducerCall(request) {
            call(request)
        }
    }
}

class TracingConsumerInterceptor<Data, Meta : Any>(
    private val queue: Queue<Data, Meta>
) : ConsumerInterceptor<Data, Meta> {

    override fun aroundReceive(
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>,
        call: (Int, ReceiveOptions<Meta>) -> List<Message<Data, Meta>>
    ): List<Message<Data, Meta>> {
        // Do we need to read OT data?
        receiveOptions.readOpenTelemetryData = when (Kolbasa.openTelemetryConfig) {
            is OpenTelemetryConfig.None -> false
            is OpenTelemetryConfig.Config -> true
        }

        return queue.queueTracing.makeConsumerCall {
            call(limit, receiveOptions)
        }
    }

}

class TracingConnectionAwareConsumerInterceptor<Data, Meta : Any>(
    private val queue: Queue<Data, Meta>
) : ConnectionAwareConsumerInterceptor<Data, Meta> {

    override fun aroundReceive(
        connection: Connection,
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>,
        call: (Connection, Int, ReceiveOptions<Meta>) -> List<Message<Data, Meta>>
    ): List<Message<Data, Meta>> {
        // Do we need to read OT data?
        receiveOptions.readOpenTelemetryData = when (Kolbasa.openTelemetryConfig) {
            is OpenTelemetryConfig.None -> false
            is OpenTelemetryConfig.Config -> true
        }

        return queue.queueTracing.makeConsumerCall {
            call(connection, limit, receiveOptions)
        }
    }

}
