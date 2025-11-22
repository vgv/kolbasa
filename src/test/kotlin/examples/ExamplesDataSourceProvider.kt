package examples

import com.zaxxer.hikari.HikariDataSource
import kolbasa.AbstractPostgresqlTest
import org.testcontainers.postgresql.PostgreSQLContainer
import javax.sql.DataSource

object ExamplesDataSourceProvider {

    /**
     * Launch PostgreSQL in Docker container using TestContainers
     */
    fun getDataSource(): DataSource {
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
     *
     * If you want to use a real PG installation:
     * 1) Comment out `getDataSource()` method above
     * 2) Uncomment this one
     * 3) Provide your own connection parameters (hostname, port, database, user, pwd)
     */
//    fun getDataSource(): DataSource {
//        val hostname = ""
//        val port = 5432
//        val database = ""
//        val user = ""
//        val pwd = ""
//
//        return HikariDataSource().apply {
//            jdbcUrl = "jdbc:postgresql://$hostname:$port/$database"
//            username = user
//            password = pwd
//        }
//    }

}
