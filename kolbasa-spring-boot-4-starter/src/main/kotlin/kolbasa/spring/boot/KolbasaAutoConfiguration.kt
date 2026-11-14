package kolbasa.spring.boot

import io.micrometer.core.instrument.MeterRegistry
import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterConsumer
import kolbasa.cluster.ClusterInspector
import kolbasa.cluster.ClusterMutator
import kolbasa.cluster.ClusterProducer
import kolbasa.cluster.ClusterTopologyUpdater
import kolbasa.consumer.datasource.Consumer
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.inspector.datasource.DatabaseInspector
import kolbasa.inspector.datasource.Inspector
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.telemetry.metrics.MetricsConfig
import kolbasa.worker.QueueWorker
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

/**
 * Spring Boot auto-configuration of kolbasa.
 *
 * Spring Boot finds it on its own, so adding the starter to the dependencies is enough. To turn it off, exclude it:
 * `@SpringBootApplication(exclude = [KolbasaAutoConfiguration::class])`.
 *
 * What it does depends on your beans:
 * - a [KolbasaStandaloneSettings] bean switches kolbasa on in standalone mode, in the application's own database: the
 *   tables and the role beans;
 * - a [KolbasaClusterSettings] bean switches kolbasa on in a cluster: the `Cluster` bean, its topology updates and
 *   the role beans;
 * - a `MeterRegistry` bean, for example the one Spring Boot Actuator makes, gives a `MetricsConfig` bean over it;
 * - every `QueueWorker` bean is started once the application is up, and stopped when it stops.
 *
 * Without them it does nothing. With both settings beans the application has two beans of every role, see
 * [KolbasaStandalone] and [KolbasaCluster].
 */
@AutoConfiguration(
    // The metrics need the MeterRegistry bean, so Spring Boot's metrics go first
    afterName = [METRICS_AUTO_CONFIGURATION, COMPOSITE_METER_REGISTRY_AUTO_CONFIGURATION]
)
class KolbasaAutoConfiguration {

    /**
     * Starts and stops the `QueueWorker` beans, see [QueueWorkersLifecycle].
     *
     * `kolbasa.workers.auto-startup=false` leaves them stopped, for example in tests. A test that needs a worker
     * starts it by hand then: `worker.start()`. Spring still closes it with the context.
     */
    @Bean
    @ConditionalOnBooleanProperty("kolbasa.workers.auto-startup", matchIfMissing = true)
    fun kolbasaWorkersLifecycle(workers: ObjectProvider<QueueWorker>): SmartLifecycle {
        return QueueWorkersLifecycle(workers)
    }

    /**
     * Kolbasa's metrics in the application's `MeterRegistry`, for example the one Spring Boot Actuator makes. Only when
     * Micrometer is on the classpath.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry::class)
    internal class Micrometer {

        /**
         * The metrics config over the application's `MeterRegistry`.
         *
         * A `MetricsConfig` bean of your own replaces it, for example `MetricsConfig.None` to switch kolbasa's metrics
         * off.
         */
        @Bean
        @ConditionalOnBean(MeterRegistry::class)
        @ConditionalOnMissingBean(MetricsConfig::class)
        fun kolbasaMetricsConfig(registry: MeterRegistry): MetricsConfig {
            return MetricsConfig.Config(registry)
        }
    }

    /**
     * Kolbasa in standalone mode, in the application's own database, switched on by a [KolbasaStandaloneSettings] bean.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(KolbasaStandaloneSettings::class)
    internal class Standalone {

        /**
         * Creates the tables of [KolbasaStandaloneSettings.queues] at startup, see
         * [KolbasaStandaloneSchemaInitializer].
         *
         * It runs after the other database initializers: Flyway, Liquibase and Spring Boot's `schema.sql`.
         *
         * `kolbasa.standalone.schema.enabled=false` switches it off, when someone else manages the schema: a DBA, or
         * Flyway.
         */
        @Bean
        @ConditionalOnBooleanProperty("kolbasa.standalone.schema.enabled", matchIfMissing = true)
        @DependsOnDatabaseInitialization
        fun kolbasaStandaloneSchemaInitializer(
            dataSource: DataSource,
            settings: KolbasaStandaloneSettings
        ): KolbasaStandaloneSchemaInitializer {
            return KolbasaStandaloneSchemaInitializer(dataSource, settings.queues)
        }

        // Every role below asks for the schema initializer and doesn't use it. Asking makes Spring create the tables
        // before the role, so a bean of yours that uses a role while it starts finds them. It is null when
        // kolbasa.standalone.schema.enabled=false
        //
        // The roles return the classes, not the interfaces. Spring Boot checks @ConditionalOnMissingBean before any
        // bean is created, so it knows only the declared types. With the classes declared, the standalone roles can
        // skip the cluster ones (ignored = Cluster*), and the cluster roles skip the standalone ones

        /**
         * The `Producer` with [KolbasaStandaloneSettings.producerOptions]. A `Producer` bean of your own replaces it,
         * unless it is a `ClusterProducer`: that one belongs to the cluster.
         */
        @Bean
        @KolbasaStandalone
        @ConditionalOnMissingBean(value = [Producer::class], ignored = [ClusterProducer::class])
        fun kolbasaStandaloneProducer(
            dataSource: DataSource,
            settings: KolbasaStandaloneSettings,
            schemaInitializer: KolbasaStandaloneSchemaInitializer?
        ): DatabaseProducer {
            return DatabaseProducer(dataSource, settings.producerOptions)
        }

        /**
         * The `Consumer` with [KolbasaStandaloneSettings.consumerOptions]. A `Consumer` bean of your own replaces it,
         * unless it is a `ClusterConsumer`: that one belongs to the cluster.
         */
        @Bean
        @KolbasaStandalone
        @ConditionalOnMissingBean(value = [Consumer::class], ignored = [ClusterConsumer::class])
        fun kolbasaStandaloneConsumer(
            dataSource: DataSource,
            settings: KolbasaStandaloneSettings,
            schemaInitializer: KolbasaStandaloneSchemaInitializer?
        ): DatabaseConsumer {
            return DatabaseConsumer(dataSource, settings.consumerOptions)
        }

        /**
         * The `Mutator` with [KolbasaStandaloneSettings.mutatorOptions]. A `Mutator` bean of your own replaces it,
         * unless it is a `ClusterMutator`: that one belongs to the cluster.
         */
        @Bean
        @KolbasaStandalone
        @ConditionalOnMissingBean(value = [Mutator::class], ignored = [ClusterMutator::class])
        fun kolbasaStandaloneMutator(
            dataSource: DataSource,
            settings: KolbasaStandaloneSettings,
            schemaInitializer: KolbasaStandaloneSchemaInitializer?
        ): DatabaseMutator {
            return DatabaseMutator(dataSource, settings.mutatorOptions)
        }

        /**
         * The `Inspector`. An `Inspector` bean of your own replaces it, unless it is a `ClusterInspector`: that one
         * belongs to the cluster.
         */
        @Bean
        @KolbasaStandalone
        @ConditionalOnMissingBean(value = [Inspector::class], ignored = [ClusterInspector::class])
        fun kolbasaStandaloneInspector(
            dataSource: DataSource,
            schemaInitializer: KolbasaStandaloneSchemaInitializer?
        ): DatabaseInspector {
            return DatabaseInspector(dataSource)
        }
    }

    /**
     * Kolbasa in a cluster, switched on by a [KolbasaClusterSettings] bean. Not named `Cluster`: that name is
     * kolbasa's class.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(KolbasaClusterSettings::class)
    internal class Clustered {

        /**
         * The cluster over [KolbasaClusterSettings.nodes]. Build your own cluster roles over it, for example
         * `ClusterProducer(cluster, options)`.
         *
         * It can be used only after [kolbasaClusterTopologyUpdater] has read the topology. A bean of yours that uses
         * the cluster while it starts must wait for it: `@DependsOn("kolbasaClusterTopologyUpdater")`.
         */
        @Bean
        fun kolbasaCluster(settings: KolbasaClusterSettings): Cluster {
            return Cluster(settings.nodes, settings.queues)
        }

        /**
         * Reads the topology of the cluster at startup, and creates the queues on every node. Then it reads the
         * topology again on the schedule of [KolbasaClusterSettings.topologyUpdaterOptions], every minute by default,
         * until the application stops.
         *
         * If the first read fails, the application doesn't start: the cluster can't work without the topology.
         */
        @Bean
        fun kolbasaClusterTopologyUpdater(cluster: Cluster, settings: KolbasaClusterSettings): ClusterTopologyUpdater {
            val updater = ClusterTopologyUpdater(cluster, settings.topologyUpdaterOptions)
            updater.start()
            return updater
        }

        // Every role below asks for the topology updater and doesn't use it. Asking makes Spring read the topology
        // before the role, so a bean of yours that uses a role while it starts finds the cluster ready.
        //
        // The roles return the classes, not the interfaces, see the comment in Standalone

        /**
         * The `Producer` with [KolbasaClusterSettings.producerOptions] and [KolbasaClusterSettings.shardStrategy]. A
         * `Producer` bean of your own replaces it, unless it is a `DatabaseProducer`: that one works with one
         * database, not with the cluster.
         */
        @Bean
        @KolbasaCluster
        @ConditionalOnMissingBean(value = [Producer::class], ignored = [DatabaseProducer::class])
        fun kolbasaClusterProducer(
            cluster: Cluster,
            settings: KolbasaClusterSettings,
            topologyUpdater: ClusterTopologyUpdater
        ): ClusterProducer {
            return ClusterProducer(cluster, settings.producerOptions, settings.shardStrategy)
        }

        /**
         * The `Consumer` with [KolbasaClusterSettings.consumerOptions]. A `Consumer` bean of your own replaces it,
         * unless it is a `DatabaseConsumer`: that one works with one database, not with the cluster.
         */
        @Bean
        @KolbasaCluster
        @ConditionalOnMissingBean(value = [Consumer::class], ignored = [DatabaseConsumer::class])
        fun kolbasaClusterConsumer(
            cluster: Cluster,
            settings: KolbasaClusterSettings,
            topologyUpdater: ClusterTopologyUpdater
        ): ClusterConsumer {
            return ClusterConsumer(cluster, settings.consumerOptions)
        }

        /**
         * The `Mutator` with [KolbasaClusterSettings.mutatorOptions]. A `Mutator` bean of your own replaces it,
         * unless it is a `DatabaseMutator`: that one works with one database, not with the cluster.
         */
        @Bean
        @KolbasaCluster
        @ConditionalOnMissingBean(value = [Mutator::class], ignored = [DatabaseMutator::class])
        fun kolbasaClusterMutator(
            cluster: Cluster,
            settings: KolbasaClusterSettings,
            topologyUpdater: ClusterTopologyUpdater
        ): ClusterMutator {
            return ClusterMutator(cluster, settings.mutatorOptions)
        }

        /**
         * The `Inspector`. An `Inspector` bean of your own replaces it, unless it is a `DatabaseInspector`: that one
         * works with one database, not with the cluster.
         */
        @Bean
        @KolbasaCluster
        @ConditionalOnMissingBean(value = [Inspector::class], ignored = [DatabaseInspector::class])
        fun kolbasaClusterInspector(
            cluster: Cluster,
            topologyUpdater: ClusterTopologyUpdater
        ): ClusterInspector {
            return ClusterInspector(cluster)
        }
    }
}

// Spring Boot's metrics auto-configurations, the ones that make the MeterRegistry bean. Strings, not classes: an application
// without Spring Boot's metrics doesn't have these classes. KolbasaMetricsTest checks them against the real classes
internal const val METRICS_AUTO_CONFIGURATION =
    "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration"
internal const val COMPOSITE_METER_REGISTRY_AUTO_CONFIGURATION =
    "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration"

