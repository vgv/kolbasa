package kolbasa.telemetry.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import kolbasa.schema.NodeId
import kolbasa.telemetry.metrics.Extensions.incInt
import kolbasa.telemetry.metrics.Extensions.observeNanos

internal class MutatorQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: MicrometerMetrics
) {

    fun mutatorMetrics(iterations: Int, mutatedMessages: Int, executionNanos: Long, byId: Boolean) {
        if (byId) {
            mutatorMutateCounterById.increment()
            mutatorMutateIterationsCounterById.incInt(iterations)
            mutatorMutateMessagesCounterById.incInt(mutatedMessages)
            mutatorMutateDurationById.observeNanos(executionNanos)
        } else {
            mutatorMutateCounterByFilter.increment()
            mutatorMutateIterationsCounterByFilter.incInt(iterations)
            mutatorMutateMessagesCounterByFilter.incInt(mutatedMessages)
            mutatorMutateDurationByFilter.observeNanos(executionNanos)
        }
    }

    private val mutatorMutateCounterById: Counter = metrics.mutatorMutateCounter
        .withTags(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateIterationsCounterById: Counter = metrics.mutatorMutateIterationsCounter
        .withTags(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateMessagesCounterById: Counter = metrics.mutatorMutateMessagesCounter
        .withTags(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateDurationById: Timer = metrics.mutatorMutateDuration
        .withTags(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)

    private val mutatorMutateCounterByFilter: Counter = metrics.mutatorMutateCounter
        .withTags(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateIterationsCounterByFilter: Counter = metrics.mutatorMutateIterationsCounter
        .withTags(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateMessagesCounterByFilter: Counter = metrics.mutatorMutateMessagesCounter
        .withTags(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateDurationByFilter: Timer = metrics.mutatorMutateDuration
        .withTags(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)

    private companion object {
        const val MUTATOR_TYPE_BY_ID = "by_id"
        const val MUTATOR_TYPE_BY_FILTER = "by_filter"
    }
}
