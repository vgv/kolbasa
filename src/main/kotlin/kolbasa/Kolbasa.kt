package kolbasa

import kolbasa.cluster.ShardStrategy
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.stats.opentelemetry.OpenTelemetryConfig
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.sql.SqlDumpConfig

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

}
