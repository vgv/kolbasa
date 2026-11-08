package kolbasa.spring.boot

import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterTopologyUpdater
import kolbasa.consumer.datasource.Consumer
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.inspector.datasource.DatabaseInspector
import kolbasa.inspector.datasource.Inspector
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import kolbasa.worker.QueueWorker
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
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
 * - a [KolbasaClusterSettings] bean switches kolbasa on in a cluster: the `Cluster` bean and its topology updates;
 * - every `QueueWorker` bean is started once the application is up, and stopped when it stops.
 *
 * Without them it does nothing.
 */
@AutoConfiguration
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

        /**
         * The `Producer` with [KolbasaStandaloneSettings.producerOptions]. A `Producer` bean of your own replaces it.
         */
        @Bean
        @ConditionalOnMissingBean(Producer::class)
        fun kolbasaStandaloneProducer(
            dataSource: DataSource,
            settings: KolbasaStandaloneSettings,
            schemaInitializer: KolbasaStandaloneSchemaInitializer?
        ): Producer {
            return DatabaseProducer(dataSource, settings.producerOptions)
        }

        /**
         * The `Consumer` with [KolbasaStandaloneSettings.consumerOptions]. A `Consumer` bean of your own replaces it.
         */
        @Bean
        @ConditionalOnMissingBean(Consumer::class)
        fun kolbasaStandaloneConsumer(
            dataSource: DataSource,
            settings: KolbasaStandaloneSettings,
            schemaInitializer: KolbasaStandaloneSchemaInitializer?
        ): Consumer {
            return DatabaseConsumer(dataSource, settings.consumerOptions)
        }

        /**
         * The `Mutator` with [KolbasaStandaloneSettings.mutatorOptions]. A `Mutator` bean of your own replaces it.
         */
        @Bean
        @ConditionalOnMissingBean(Mutator::class)
        fun kolbasaStandaloneMutator(
            dataSource: DataSource,
            settings: KolbasaStandaloneSettings,
            schemaInitializer: KolbasaStandaloneSchemaInitializer?
        ): Mutator {
            return DatabaseMutator(dataSource, settings.mutatorOptions)
        }

        /**
         * The `Inspector`. An `Inspector` bean of your own replaces it.
         */
        @Bean
        @ConditionalOnMissingBean(Inspector::class)
        fun kolbasaStandaloneInspector(
            dataSource: DataSource,
            schemaInitializer: KolbasaStandaloneSchemaInitializer?
        ): Inspector {
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
         * topology again every minute, until the application stops.
         *
         * If the first read fails, the application doesn't start: the cluster can't work without the topology.
         */
        @Bean
        fun kolbasaClusterTopologyUpdater(cluster: Cluster): ClusterTopologyUpdater {
            val updater = ClusterTopologyUpdater(cluster)
            updater.start()
            return updater
        }
    }
}
