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
        val hostname = System.getenv("host")
        val port = System.getenv("port")?.toIntOrNull() ?: 5432
        val database = System.getenv("database")
        val user = System.getenv("user") ?: "postgres"
        val pwd = System.getenv("password") ?: ""

        return HikariDataSource().apply {
            jdbcUrl = "jdbc:postgresql://$hostname:$port/$database"
            username = user
            password = pwd
        }
    }

}
