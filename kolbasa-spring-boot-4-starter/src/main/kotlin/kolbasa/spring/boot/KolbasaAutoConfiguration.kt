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
 * - a [LocalKolbasaSettings] bean switches kolbasa on in the application's own database: the tables and the role beans;
 * - a [ClusterKolbasaSettings] bean switches kolbasa on in a cluster: the `Cluster` bean and its topology updates;
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
     * Kolbasa in the application's own database, switched on by a [LocalKolbasaSettings] bean.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(LocalKolbasaSettings::class)
    internal class Local {

        /**
         * Creates the tables of the local queues at startup, see [LocalKolbasaSchemaInitializer].
         *
         * It runs after the other database initializers: Flyway, Liquibase and Spring Boot's `schema.sql`.
         *
         * `kolbasa.schema.enabled=false` switches it off, when someone else manages the schema: a DBA, or Flyway.
         */
        @Bean
        @ConditionalOnBooleanProperty("kolbasa.schema.enabled", matchIfMissing = true)
        @DependsOnDatabaseInitialization
        fun localKolbasaSchemaInitializer(
            dataSource: DataSource,
            settings: LocalKolbasaSettings
        ): LocalKolbasaSchemaInitializer {
            return LocalKolbasaSchemaInitializer(dataSource, settings.queues)
        }

        // Every role below asks for the schema initializer and doesn't use it. Asking makes Spring create the tables
        // before the role, so a bean of yours that uses a role while it starts finds them. It is null when
        // kolbasa.schema.enabled=false

        /**
         * The `Producer` with [LocalKolbasaSettings.producerOptions]. A `Producer` bean of your own replaces it.
         */
        @Bean
        @ConditionalOnMissingBean(Producer::class)
        fun localKolbasaProducer(
            dataSource: DataSource,
            settings: LocalKolbasaSettings,
            schemaInitializer: LocalKolbasaSchemaInitializer?
        ): Producer {
            return DatabaseProducer(dataSource, settings.producerOptions)
        }

        /**
         * The `Consumer` with [LocalKolbasaSettings.consumerOptions]. A `Consumer` bean of your own replaces it.
         */
        @Bean
        @ConditionalOnMissingBean(Consumer::class)
        fun localKolbasaConsumer(
            dataSource: DataSource,
            settings: LocalKolbasaSettings,
            schemaInitializer: LocalKolbasaSchemaInitializer?
        ): Consumer {
            return DatabaseConsumer(dataSource, settings.consumerOptions)
        }

        /**
         * The `Mutator` with [LocalKolbasaSettings.mutatorOptions]. A `Mutator` bean of your own replaces it.
         */
        @Bean
        @ConditionalOnMissingBean(Mutator::class)
        fun localKolbasaMutator(
            dataSource: DataSource,
            settings: LocalKolbasaSettings,
            schemaInitializer: LocalKolbasaSchemaInitializer?
        ): Mutator {
            return DatabaseMutator(dataSource, settings.mutatorOptions)
        }

        /**
         * The `Inspector`. An `Inspector` bean of your own replaces it.
         */
        @Bean
        @ConditionalOnMissingBean(Inspector::class)
        fun localKolbasaInspector(
            dataSource: DataSource,
            schemaInitializer: LocalKolbasaSchemaInitializer?
        ): Inspector {
            return DatabaseInspector(dataSource)
        }
    }

    /**
     * Kolbasa in a cluster, switched on by a [ClusterKolbasaSettings] bean. Not named `Cluster`: that name is
     * kolbasa's class.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(ClusterKolbasaSettings::class)
    internal class Clustered {

        /**
         * The cluster over [ClusterKolbasaSettings.nodes]. Build your own cluster roles over it, for example
         * `ClusterProducer(cluster, options)`.
         *
         * It can be used only after [clusterKolbasaTopologyUpdater] has read the topology. A bean of yours that uses
         * the cluster while it starts must wait for it: `@DependsOn("clusterKolbasaTopologyUpdater")`.
         */
        @Bean
        fun kolbasaCluster(settings: ClusterKolbasaSettings): Cluster {
            return Cluster(settings.nodes, settings.queues)
        }

        /**
         * Reads the topology of the cluster at startup, and creates the queues on every node. Then it reads the
         * topology again every minute, until the application stops.
         *
         * If the first read fails, the application doesn't start: the cluster can't work without the topology.
         */
        @Bean
        fun clusterKolbasaTopologyUpdater(cluster: Cluster): ClusterTopologyUpdater {
            val updater = ClusterTopologyUpdater(cluster)
            updater.start()
            return updater
        }
    }
}
