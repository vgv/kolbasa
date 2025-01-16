package kolbasa.stats.opentelemetry

import kolbasa.Kolbasa
import kolbasa.consumer.Message
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.ConsumerInterceptor
import kolbasa.queue.Queue

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
