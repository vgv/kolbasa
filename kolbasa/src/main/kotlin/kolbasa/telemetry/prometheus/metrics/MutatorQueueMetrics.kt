package kolbasa.telemetry.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.schema.NodeId
import kolbasa.telemetry.prometheus.metrics.Extensions.incInt
import kolbasa.telemetry.prometheus.metrics.Extensions.observeNanos

internal class MutatorQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    metrics: PrometheusMetrics
) {

    fun mutatorMetrics(iterations: Int, mutatedMessages: Int, executionNanos: Long, byId: Boolean) {
        if (byId) {
            mutatorMutateCounterById.inc()
            mutatorMutateIterationsCounterById.incInt(iterations)
            mutatorMutateMessagesCounterById.incInt(mutatedMessages)
            mutatorMutateDurationById.observeNanos(executionNanos)
        } else {
            mutatorMutateCounterByFilter.inc()
            mutatorMutateIterationsCounterByFilter.incInt(iterations)
            mutatorMutateMessagesCounterByFilter.incInt(mutatedMessages)
            mutatorMutateDurationByFilter.observeNanos(executionNanos)
        }
    }

    private val mutatorMutateCounterById: CounterDataPoint = metrics.mutatorMutateCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateIterationsCounterById: CounterDataPoint = metrics.mutatorMutateIterationsCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateMessagesCounterById: CounterDataPoint = metrics.mutatorMutateMessagesCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateDurationById: DistributionDataPoint = metrics.mutatorMutateDuration
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)

    private val mutatorMutateCounterByFilter: CounterDataPoint = metrics.mutatorMutateCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateIterationsCounterByFilter: CounterDataPoint = metrics.mutatorMutateIterationsCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateMessagesCounterByFilter: CounterDataPoint = metrics.mutatorMutateMessagesCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateDurationByFilter: DistributionDataPoint = metrics.mutatorMutateDuration
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)

    private companion object {
        const val MUTATOR_TYPE_BY_ID = "by_id"
        const val MUTATOR_TYPE_BY_FILTER = "by_filter"
    }
}
