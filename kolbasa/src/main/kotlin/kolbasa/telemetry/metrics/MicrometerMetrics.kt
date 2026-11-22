package kolbasa.telemetry.metrics

import io.micrometer.core.instrument.MeterRegistry
import kolbasa.queue.Queue
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * All metrics of the library in one registry.
 *
 * There is one such object per registry, [MetricsConfig.Config] makes it on the first call and keeps it. So every
 * config over a registry shares the meters of a queue, and reads the queue gauges on the same schedule.
 */
internal class MicrometerMetrics(
    // Weak, see MetricsConfig.Config. The families keep it weak too
    private val registry: WeakReference<MeterRegistry>
) {

    // Performance: all meters of a queue are made once, with their tags (queue name etc.), and kept here
    //
    // One entry per queue in the registry, so every config over it reads the queue gauges from the database on the
    // same schedule
    private val queues: ConcurrentMap<String, MicrometerQueueMetrics> = ConcurrentHashMap()

    fun queueMetrics(queue: Queue<*>): MicrometerQueueMetrics {
        return queues.computeIfAbsent(queue.name) { MicrometerQueueMetrics(queue, this) }
    }

    // ----------------------------------------------------------------------------
    // Queue metrics
    val queueMessagesGauge = GaugeFamily(
        registry,
        name = "kolbasa.queue.messages",
        description = "Amount of messages in the queue in different states (scheduled, ready, in-flight, retry, dead)",
        tagKeys = listOf("queue", "state", "node_id")
    )

    val queueMessageAgesGauge = GaugeFamily(
        registry,
        name = "kolbasa.queue.message.age",
        description = "Queue messages ages in seconds – oldest, newest and oldest ready",
        tagKeys = listOf("queue", "type", "node_id"),
        baseUnit = "seconds"
    )

    val queueSizeGauge = GaugeFamily(
        registry,
        name = "kolbasa.queue.size",
        description = "Queue size in bytes",
        tagKeys = listOf("queue", "node_id"),
        baseUnit = "bytes"
    )

    // ----------------------------------------------------------------------------
    // Producer metrics
    val producerSendCounter = CounterFamily(
        registry,
        name = "kolbasa.producer.send",
        description = "Amount of producer send() calls",
        tagKeys = listOf("queue", "partial_insert_type", "node_id")
    )

    val producerSendMessagesCounter = CounterFamily(
        registry,
        name = "kolbasa.producer.send.messages",
        description = "Amount of all (successful and failed) messages sent by producer send() calls",
        tagKeys = listOf("queue", "partial_insert_type", "node_id")
    )

    val producerSendFailedMessagesCounter = CounterFamily(
        registry,
        name = "kolbasa.producer.send.messages.failed",
        description = "Amount of failed messages sent by producer send() calls",
        tagKeys = listOf("queue", "partial_insert_type", "node_id")
    )

    val producerSendDuration = TimerFamily(
        registry,
        name = "kolbasa.producer.send.duration",
        description = "Producer send() calls duration",
        tagKeys = listOf("queue", "partial_insert_type", "node_id")
    )

    val producerSendBytesCounter = CounterFamily(
        registry,
        name = "kolbasa.producer.send.bytes",
        description = "Amount of bytes sent by producer send() calls",
        tagKeys = listOf("queue", "partial_insert_type", "node_id"),
        baseUnit = "bytes"
    )

    // ----------------------------------------------------------------------------
    // Consumer metrics
    // Receive
    val consumerReceiveCounter = CounterFamily(
        registry,
        name = "kolbasa.consumer.receive",
        description = "Amount of consumer receive() calls",
        tagKeys = listOf("queue", "node_id")
    )

    val consumerReceiveBytesCounter = CounterFamily(
        registry,
        name = "kolbasa.consumer.receive.bytes",
        description = "Amount of bytes read by consumer receive() calls",
        tagKeys = listOf("queue", "node_id"),
        baseUnit = "bytes"
    )

    val consumerReceiveMessagesCounter = CounterFamily(
        registry,
        name = "kolbasa.consumer.receive.messages",
        description = "Amount of messages received by consumer receive() calls",
        tagKeys = listOf("queue", "node_id")
    )

    val consumerReceiveUnreadableMessagesCounter = CounterFamily(
        registry,
        name = "kolbasa.consumer.receive.messages.unreadable",
        description = "Amount of received messages that can't be read and were passed to the ReadErrorHandler",
        tagKeys = listOf("queue", "node_id")
    )

    val consumerReceiveDuration = TimerFamily(
        registry,
        name = "kolbasa.consumer.receive.duration",
        description = "Consumer receive() calls duration",
        tagKeys = listOf("queue", "node_id")
    )

    // Delete
    val consumerDeleteCounter = CounterFamily(
        registry,
        name = "kolbasa.consumer.delete",
        description = "Amount of consumer delete() calls",
        tagKeys = listOf("queue", "node_id")
    )

    val consumerDeleteMessagesCounter = CounterFamily(
        registry,
        name = "kolbasa.consumer.delete.messages",
        description = "Amount of messages removed by consumer delete() calls",
        tagKeys = listOf("queue", "node_id")
    )

    val consumerDeleteDuration = TimerFamily(
        registry,
        name = "kolbasa.consumer.delete.duration",
        description = "Consumer delete() calls duration",
        tagKeys = listOf("queue", "node_id")
    )

    // ----------------------------------------------------------------------------
    // Sweep metrics
    val sweepCounter = CounterFamily(
        registry,
        name = "kolbasa.sweep",
        description = "Number of sweeps",
        tagKeys = listOf("queue", "node_id")
    )

    val sweepMessagesRemovedCounter = CounterFamily(
        registry,
        name = "kolbasa.sweep.removed.messages",
        description = "Number of messages removed by sweep",
        tagKeys = listOf("queue", "node_id")
    )

    val sweepDuration = TimerFamily(
        registry,
        name = "kolbasa.sweep.duration",
        description = "Sweep duration",
        tagKeys = listOf("queue", "node_id")
    )

    // ----------------------------------------------------------------------------
    // Mutator metrics
    val mutatorMutateCounter = CounterFamily(
        registry,
        name = "kolbasa.mutator.mutate",
        description = "Number of mutates",
        tagKeys = listOf("queue", "node_id", "type")
    )

    val mutatorMutateIterationsCounter = CounterFamily(
        registry,
        name = "kolbasa.mutator.mutate.iterations",
        description = "Number of mutate iterations (every mutate can have multiple iterations)",
        tagKeys = listOf("queue", "node_id", "type")
    )

    val mutatorMutateMessagesCounter = CounterFamily(
        registry,
        name = "kolbasa.mutator.mutated.messages",
        description = "Number of mutated messages",
        tagKeys = listOf("queue", "node_id", "type")
    )

    val mutatorMutateDuration = TimerFamily(
        registry,
        name = "kolbasa.mutator.mutate.duration",
        description = "Mutate duration",
        tagKeys = listOf("queue", "node_id", "type")
    )
}
