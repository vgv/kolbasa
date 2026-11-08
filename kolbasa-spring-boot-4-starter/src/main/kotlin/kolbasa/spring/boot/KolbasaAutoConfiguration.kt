package kolbasa.spring.boot

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization
import org.springframework.context.annotation.Bean
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
     * Creates the tables of the local queues at startup, see [LocalKolbasaSchemaInitializer].
     *
     * It runs after the other database initializers: Flyway, Liquibase and Spring Boot's `schema.sql`.
     *
     * `kolbasa.schema.enabled=false` switches it off, when someone else manages the schema: a DBA, or Flyway.
     */
    @Bean
    @ConditionalOnBean(LocalKolbasaSettings::class)
    @ConditionalOnBooleanProperty("kolbasa.schema.enabled", matchIfMissing = true)
    @DependsOnDatabaseInitialization
    fun localKolbasaSchemaInitializer(
        dataSource: DataSource,
        settings: LocalKolbasaSettings
    ): LocalKolbasaSchemaInitializer {
        return LocalKolbasaSchemaInitializer(dataSource, settings.queues)
    }
}
