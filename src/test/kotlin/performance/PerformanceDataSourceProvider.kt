package performance

import com.zaxxer.hikari.HikariDataSource
import kolbasa.AbstractPostgresqlTest
import org.testcontainers.postgresql.PostgreSQLContainer
import javax.sql.DataSource

object PerformanceDataSourceProvider {

    fun internalDatasource(): DataSource {
        val pgContainer = PostgreSQLContainer(AbstractPostgresqlTest.NEWEST_POSTGRES_IMAGE.dockerImage)

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
