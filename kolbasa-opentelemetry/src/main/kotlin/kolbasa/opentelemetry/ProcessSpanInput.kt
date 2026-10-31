package kolbasa.opentelemetry

import io.opentelemetry.api.trace.SpanContext
import kolbasa.consumer.Message

internal class ProcessSpanInput(
    val queueName: String,
    val message: Message<*>,
    // Trace context the sender stored with the message
    val traceContext: Map<String, String>,
    // The span of whoever called QueueProcessor.process(), invalid if there is none
    val callerSpanContext: SpanContext
)
