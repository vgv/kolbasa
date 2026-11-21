package kolbasa

import kolbasa.consumer.sweep.SweepConfig
import kolbasa.telemetry.metrics.MetricsConfig
import kolbasa.telemetry.sql.SqlDumpConfig
import kolbasa.telemetry.tracing.Tracing
import kolbasa.utils.Threads
import java.util.concurrent.Executor

/**
 * Global, process-wide Kolbasa settings.
 *
 * Every property here is one value shared by all producers, consumers, mutators and queues in the JVM. All of them
 * can be changed at any time. See each property for WHEN it is read.
 *
 * ## Usage Example
 *
 * ```kotlin
 * Kolbasa.metricsConfig = MetricsConfig.Config(meterRegistry)
 * Kolbasa.sweepConfig = SweepConfig.builder().probability(0.001).build()
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setMetricsConfig(new MetricsConfig.Config(meterRegistry));
 * Kolbasa.setSweepConfig(SweepConfig.builder().probability(0.001).build());
 * ```
 */
object Kolbasa {

    /**
     * Sweep configuration – the probabilistic cleanup that runs while messages are received.
     *
     * Read on every receive(), so a change takes effect on the next call.
     */
    @JvmStatic
    @Volatile
    var sweepConfig: SweepConfig = SweepConfig()

    /**
     * SQL dump configuration – which statements are printed, for which queues, and where they are written.
     *
     * Read on every statement, so a change takes effect immediately – and that is exactly what makes it dangerous.
     * With many queues and a high send/receive rate one assignment can turn on a flood of dumps at once. Either be
     * ready for that volume, or keep it narrow: register only the queues and only the
     * [StatementKind][kolbasa.telemetry.sql.StatementKind]s you really need to look at.
     */
    @JvmStatic
    @Volatile
    var sqlDumpConfig: SqlDumpConfig = SqlDumpConfig()

    /**
     * Metrics for every producer, consumer and mutator that doesn't set its own in
     * [ProducerOptions.metricsConfig][kolbasa.producer.ProducerOptions.metricsConfig],
     * [ConsumerOptions.metricsConfig][kolbasa.consumer.ConsumerOptions.metricsConfig] or
     * [MutatorOptions.metricsConfig][kolbasa.mutator.MutatorOptions.metricsConfig].
     *
     * The default, [MetricsConfig.None], collects nothing.
     *
     * Read on every call of a producer, consumer or mutator, and on every sweep, so a change takes effect on the
     * next call.
     */
    @JvmStatic
    @Volatile
    var metricsConfig: MetricsConfig = MetricsConfig.None

    /**
     * Tracing for every producer and consumer that doesn't set its own in
     * [ProducerOptions.tracing][kolbasa.producer.ProducerOptions.tracing] or
     * [ConsumerOptions.tracing][kolbasa.consumer.ConsumerOptions.tracing].
     *
     * The default, [Tracing.NONE], traces nothing.
     *
     * Read on every send() and receive(), so a change takes effect on the next call.
     */
    @JvmStatic
    @Volatile
    var tracing: Tracing = Tracing.NONE

    /**
     * Executor for every `sendAsync()` and `mutateAsync()` call that doesn't set its own in
     * [SendOptions.asyncExecutor][kolbasa.producer.SendOptions.asyncExecutor],
     * [ProducerOptions.asyncExecutor][kolbasa.producer.ProducerOptions.asyncExecutor] or
     * [MutatorOptions.asyncExecutor][kolbasa.mutator.MutatorOptions.asyncExecutor].
     *
     * The default is one background (daemon) thread for the whole JVM. The calls run on it one by one, in the order
     * they were made. The thread starts with the first call and ends after a minute without calls; the next call
     * starts it again. One thread is enough for most applications: the parallelism comes from many clients working at
     * once, not from one client.
     *
     * Know what this default means:
     * - All these calls share the thread. A call that hangs, for example on a node that doesn't answer, holds up all
     *   the others, for as long as the connection and socket timeouts allow.
     * - The queue of the thread has no limit. If you call `sendAsync()` faster than the database takes the messages,
     *   they pile up in memory.
     * - Nobody shuts the executor down. When the JVM exits, the calls still in the queue are lost, and their futures
     *   never complete.
     *
     * If async calls matter to your application, set your own executor here, once, at startup: with the number of
     * threads, the queue limit and the monitoring you need, and shut it down when your application stops. Any
     * [Executor] fits, Spring's `TaskExecutor` too.
     *
     * Read on every async call, so a change takes effect on the next call.
     */
    @JvmStatic
    @Volatile
    var asyncExecutor: Executor = Threads.newAsyncExecutor()

}
