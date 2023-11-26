package performance

import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.containers.PostgreSQLContainer
import javax.sql.DataSource

object PerformanceDataSourceProvider {

    fun internalDatasource(): DataSource {
        val pgContainer = PostgreSQLContainer("postgres:16.1-alpine")

        // Start PG container
        pgContainer.start()

        // Init dataSource
        return HikariDataSource().apply {
            jdbcUrl = pgContainer.jdbcUrl
            username = pgContainer.username
            password = pgContainer.password
        }
    }

    /**
     * Use external PostgreSQL installation
     */
    fun externalDatasource(): DataSource {
        return HikariDataSource().apply {
            jdbcUrl = "jdbc:postgresql://${Env.pgHostname}:${Env.pgPort}/${Env.pgDatabase}"
            username = Env.pgUser
            password = Env.pgPassword
        }
    }

}
