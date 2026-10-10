package kolbasa.example

import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.postgresql.PostgreSQLContainer
import javax.sql.DataSource

object ExamplesDataSourceProvider {

    /**
     * The examples are documentation first: anyone must be able to copy one into their own project and
     * run it. So the image is spelled out here instead of being taken from the library's test
     * infrastructure - `kolbasa.AbstractPostgresqlTest` is not something a user of kolbasa has.
     *
     * The test suite itself runs against every version in
     * kolbasa/src/test/resources/postgresql-test-images.txt; an example only needs a recent PostgreSQL.
     */
    private const val POSTGRES_IMAGE = "postgres:18.6-alpine"


    /**
     * Launch PostgreSQL in Docker container using TestContainers
     */
    @JvmStatic
    fun getDataSource(): DataSource {
        val pgContainer = PostgreSQLContainer(POSTGRES_IMAGE)

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
//    @JvmStatic
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
