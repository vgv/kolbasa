package kolbasa.spring.boot

import kolbasa.consumer.datasource.Consumer
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.inspector.datasource.DatabaseInspector
import kolbasa.inspector.datasource.Inspector
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.mutator.datasource.Mutator
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.producer.datasource.Producer
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

/**
 * Spring Boot auto-configuration of kolbasa.
 *
 * Spring Boot finds it on its own, so adding the starter to the dependencies is enough. To turn it off, exclude it:
 * `@SpringBootApplication(exclude = [KolbasaAutoConfiguration::class])`.
 *
 * It does nothing on its own: kolbasa is switched on by a [LocalKolbasaSettings] bean.
 */
@AutoConfiguration
class KolbasaAutoConfiguration {

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
}
