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

object Kolbasa {

    @JvmStatic
    @Volatile
    var sweepConfig: SweepConfig = SweepConfig()

    @JvmStatic
    @Volatile
    var shardStrategy: ShardStrategy = ShardStrategy.ThreadLocalWithInterval()

    @JvmStatic
    @Volatile
    var sqlDumpConfig: SqlDumpConfig = SqlDumpConfig()

    @JvmStatic
    @Volatile
    var prometheusConfig: PrometheusConfig = PrometheusConfig.None

    @JvmStatic
    @Volatile
    var openTelemetryConfig: OpenTelemetryConfig = OpenTelemetryConfig.None

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
