package kolbasa.telemetry.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import kolbasa.producer.PartialInsert
import kolbasa.schema.NodeId
import kolbasa.telemetry.metrics.Extensions.incInt
import kolbasa.telemetry.metrics.Extensions.incLong
import kolbasa.telemetry.metrics.Extensions.observeNanos

internal class ProducerQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: MicrometerMetrics
) {

    fun producerSendMetrics(
        partialInsert: PartialInsert,
        allMessages: Int,
        failedMessages: Int,
        executionNanos: Long,
        bytesSent: Long
    ) {
        when (partialInsert) {
            PartialInsert.PROHIBITED -> {
                producerSendCounterProhibited.increment()
                producerSendMessagesCounterProhibited.incInt(allMessages)
                producerSendFailedMessagesCounterProhibited.incInt(failedMessages)
                producerSendDurationProhibited.observeNanos(executionNanos)
                producerSendBytesCounterProhibited.incLong(bytesSent)
            }

            PartialInsert.UNTIL_FIRST_FAILURE -> {
                producerSendCounterUntilFirstFailure.increment()
                producerSendMessagesCounterUntilFirstFailure.incInt(allMessages)
                producerSendFailedMessagesCounterUntilFirstFailure.incInt(failedMessages)
                producerSendDurationUntilFirstFailure.observeNanos(executionNanos)
                producerSendBytesCounterUntilFirstFailure.incLong(bytesSent)
            }

            PartialInsert.INSERT_AS_MANY_AS_POSSIBLE -> {
                producerSendCounterInsertAsManyAsPossible.increment()
                producerSendMessagesCounterInsertAsManyAsPossible.incInt(allMessages)
                producerSendFailedMessagesCounterInsertAsManyAsPossible.incInt(failedMessages)
                producerSendDurationInsertAsManyAsPossible.observeNanos(executionNanos)
                producerSendBytesCounterInsertAsManyAsPossible.incLong(bytesSent)
            }
        }
    }

    private val producerSendCounterProhibited: Counter = metrics.producerSendCounter
        .withTags(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendCounterUntilFirstFailure: Counter = metrics.producerSendCounter
        .withTags(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendCounterInsertAsManyAsPossible: Counter = metrics.producerSendCounter
        .withTags(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendMessagesCounterProhibited: Counter = metrics.producerSendMessagesCounter
        .withTags(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendMessagesCounterUntilFirstFailure: Counter = metrics.producerSendMessagesCounter
        .withTags(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendMessagesCounterInsertAsManyAsPossible: Counter = metrics.producerSendMessagesCounter
        .withTags(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendFailedMessagesCounterProhibited: Counter =
        metrics.producerSendFailedMessagesCounter
            .withTags(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendFailedMessagesCounterUntilFirstFailure: Counter =
        metrics.producerSendFailedMessagesCounter
            .withTags(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendFailedMessagesCounterInsertAsManyAsPossible: Counter =
        metrics.producerSendFailedMessagesCounter
            .withTags(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendDurationProhibited: Timer = metrics.producerSendDuration
        .withTags(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendDurationUntilFirstFailure: Timer = metrics.producerSendDuration
        .withTags(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendDurationInsertAsManyAsPossible: Timer = metrics.producerSendDuration
        .withTags(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)

    private val producerSendBytesCounterProhibited: Counter = metrics.producerSendBytesCounter
        .withTags(queueName, PartialInsert.PROHIBITED.name, nodeId.id)
    private val producerSendBytesCounterUntilFirstFailure: Counter = metrics.producerSendBytesCounter
        .withTags(queueName, PartialInsert.UNTIL_FIRST_FAILURE.name, nodeId.id)
    private val producerSendBytesCounterInsertAsManyAsPossible: Counter = metrics.producerSendBytesCounter
        .withTags(queueName, PartialInsert.INSERT_AS_MANY_AS_POSSIBLE.name, nodeId.id)
}
