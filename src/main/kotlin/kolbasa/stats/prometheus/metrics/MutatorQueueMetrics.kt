package kolbasa.stats.prometheus.metrics

import io.prometheus.metrics.core.datapoints.CounterDataPoint
import io.prometheus.metrics.core.datapoints.DistributionDataPoint
import kolbasa.schema.NodeId
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.metrics.Extensions.incInt
import kolbasa.stats.prometheus.metrics.Extensions.observeNanos

internal class MutatorQueueMetrics(
    queueName: String,
    nodeId: NodeId,
    prometheusConfig: PrometheusConfig.Config
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

    private val mutatorMutateCounterById: CounterDataPoint = prometheusConfig.mutatorMutateCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateIterationsCounterById: CounterDataPoint = prometheusConfig.mutatorMutateIterationsCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateMessagesCounterById: CounterDataPoint = prometheusConfig.mutatorMutateMessagesCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)
    private val mutatorMutateDurationById: DistributionDataPoint = prometheusConfig.mutatorMutateDuration
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_ID)

    private val mutatorMutateCounterByFilter: CounterDataPoint = prometheusConfig.mutatorMutateCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateIterationsCounterByFilter: CounterDataPoint = prometheusConfig.mutatorMutateIterationsCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateMessagesCounterByFilter: CounterDataPoint = prometheusConfig.mutatorMutateMessagesCounter
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)
    private val mutatorMutateDurationByFilter: DistributionDataPoint = prometheusConfig.mutatorMutateDuration
        .labelValues(queueName, nodeId.id, MUTATOR_TYPE_BY_FILTER)

    private companion object {
        const val MUTATOR_TYPE_BY_ID = "by_id"
        const val MUTATOR_TYPE_BY_FILTER = "by_filter"
    }
}
