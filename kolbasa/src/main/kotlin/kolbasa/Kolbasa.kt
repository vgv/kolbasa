package kolbasa

import kolbasa.cluster.ClusterStateUpdateConfig
import kolbasa.cluster.ShardStrategy
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.stats.opentelemetry.OpenTelemetryConfig
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.sql.SqlDumpConfig
import kolbasa.utils.DaemonThreadFactory
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Global, process-wide Kolbasa settings.
 *
 * Every property here is one value shared by all producers, consumers, mutators and queues in the JVM. All of them
 * can be changed at any time, but see each property for WHEN it is read: most are read at the point of use, while
 * [prometheusConfig] and [openTelemetryConfig] have to be set before any other use of Kolbasa.
 *
 * ## Usage Example
 *
 * ```kotlin
 * Kolbasa.prometheusConfig = PrometheusConfig.Config()
 * Kolbasa.sweepConfig = SweepConfig.builder().probability(0.001).build()
 * ```
 *
 * The same from Java:
 *
 * ```java
 * Kolbasa.setPrometheusConfig(new PrometheusConfig.Config());
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
     * Strategy that picks a shard for a message when neither the send() call nor the producer sets one.
     *
     * Only a [Cluster][kolbasa.cluster.Cluster] producer uses it, and it is read on every send().
     */
    @JvmStatic
    @Volatile
    var shardStrategy: ShardStrategy = ShardStrategy.ThreadLocalWithInterval()

    /**
     * SQL dump configuration – which statements are printed, for which queues, and where they are written.
     *
     * Read on every statement, so a change takes effect immediately – and that is exactly what makes it dangerous.
     * With many queues and a high send/receive rate one assignment can turn on a flood of dumps at once. Either be
     * ready for that volume, or keep it narrow: register only the queues and only the
     * [StatementKind][kolbasa.stats.sql.StatementKind]s you really need to look at.
     */
    @JvmStatic
    @Volatile
    var sqlDumpConfig: SqlDumpConfig = SqlDumpConfig()

    /**
     * Prometheus metrics configuration. The default, [PrometheusConfig.None], collects nothing.
     *
     * Set it before any other use of Kolbasa: the configuration is applied once and stays for the whole life of
     * the JVM, so changing it later has no effect on anything already running.
     */
    @JvmStatic
    @Volatile
    var prometheusConfig: PrometheusConfig = PrometheusConfig.None

    /**
     * OpenTelemetry configuration. The default, [OpenTelemetryConfig.None], collects and propagates nothing.
     *
     * Set it before any other use of Kolbasa: the configuration is applied once and stays for the whole life of
     * the JVM, so changing it later has no effect on anything already running.
     */
    @JvmStatic
    @Volatile
    var openTelemetryConfig: OpenTelemetryConfig = OpenTelemetryConfig.None

    /**
     * How often a [Cluster][kolbasa.cluster.Cluster] refreshes its view of the nodes, and on which executor.
     *
     * Read again before every reschedule, so a change affects the next cycle.
     */
    @JvmStatic
    @Volatile
    var clusterStateUpdateConfig: ClusterStateUpdateConfig = ClusterStateUpdateConfig()

    /**
     * Default executor for async operations
     *
     * By default, it's a single-thread executor because
     * 1) It looks like a good idea to keep time order of operations
     * 2) 99% of Kolbasa usage will work when many clients are connected to several real PostgreSQL servers, so there is no point
     *    in increasing parallelism for one client, many clients will create many simultaneous requests to several servers anyway.
     *    For cases when you know exactly your use case, for example, one producer for a cluster of PostgreSQL servers,
     *    each containing a few physical disks, you can configure the required parallelism accordingly.
     *
     * If you want to use a custom executor for a specific [Producer][kolbasa.producer.datasource.Producer] or
     * [Mutator][kolbasa.mutator.datasource.Mutator], please look at
     * [ProducerOptions.asyncExecutor][kolbasa.producer.ProducerOptions.asyncExecutor] or
     * [MutatorOptions.asyncExecutor][kolbasa.mutator.MutatorOptions.asyncExecutor]
     */
    @JvmStatic
    @Volatile
    var asyncExecutor: ExecutorService = Executors.newSingleThreadExecutor(DaemonThreadFactory("kolbasa-async"))

}
