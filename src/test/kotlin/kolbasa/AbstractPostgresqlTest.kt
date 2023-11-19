package kolbasa

import com.zaxxer.hikari.HikariDataSource
import kolbasa.pg.DatabaseExtensions.useStatement
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import javax.sql.DataSource

@Tag("unit-db")
abstract class AbstractPostgresqlTest {

    @Container
    protected val pgContainer = PostgreSQLContainer(CURRENT_IMAGE)

    protected lateinit var dataSource: DataSource
    protected lateinit var dataSourceFirstSchema: DataSource
    protected lateinit var dataSourceSecondSchema: DataSource

    @BeforeEach
    fun init() {
        // Start PG container
        pgContainer.start()

        // Init dataSource, public schema
        dataSource = HikariDataSource().apply {
            jdbcUrl = pgContainer.jdbcUrl
            username = pgContainer.username
            password = pgContainer.password
        }

        // Init dataSource, first schema
        dataSourceFirstSchema = HikariDataSource().apply {
            jdbcUrl = pgContainer.jdbcUrl
            username = pgContainer.username
            password = pgContainer.password
            schema = FIRST_SCHEMA_NAME
        }

        // Init dataSource, second schema
        dataSourceSecondSchema = HikariDataSource().apply {
            jdbcUrl = pgContainer.jdbcUrl
            username = pgContainer.username
            password = pgContainer.password
            schema = SECOND_SCHEMA_NAME
        }

        // Create all schemas
        dataSource.useStatement { statement ->
            statement.execute("create schema $FIRST_SCHEMA_NAME")
            statement.execute("create schema $SECOND_SCHEMA_NAME")
        }

        // Insert test data for all schemas, if any
        dataSource.connection.use { connection ->
            connection.autoCommit = true // execute all statements in a separate transaction for each statement
            connection.useStatement { statement ->
                generateTestData().forEach(statement::execute)
            }
        }
        dataSourceFirstSchema.connection.use { connection ->
            connection.autoCommit = true // execute all statements in a separate transaction for each statement
            connection.useStatement { statement ->
                generateTestDataFirstSchema().forEach(statement::execute)
            }
        }
        dataSourceSecondSchema.connection.use { connection ->
            connection.autoCommit = true // execute all statements in a separate transaction for each statement
            connection.useStatement { statement ->
                generateTestDataSecondSchema().forEach(statement::execute)
            }
        }
    }

    @AfterEach
    fun shutdown() {
        (dataSource as HikariDataSource).close()
        (dataSourceFirstSchema as HikariDataSource).close()
        (dataSourceSecondSchema as HikariDataSource).close()

        pgContainer.stop()
    }

    protected open fun generateTestData(): List<String> {
        return emptyList()
    }

    protected open fun generateTestDataFirstSchema(): List<String> {
        return emptyList()
    }

    protected open fun generateTestDataSecondSchema(): List<String> {
        return emptyList()
    }

    companion object {
        const val FIRST_SCHEMA_NAME = "first"
        const val SECOND_SCHEMA_NAME = "second"

        // All PG images to run tests
        // Choose random image at every run
        private val POSTGRES_IMAGES = setOf(
            "postgres:10.23-alpine",
            "postgres:11.22-alpine",
            "postgres:12.17-alpine",
            "postgres:13.13-alpine",
            "postgres:14.10-alpine",
            "postgres:15.5-alpine",
            "postgres:16.1-alpine"
        )

        private val CURRENT_IMAGE = POSTGRES_IMAGES.random()

        init {
            println("PostgreSQL docker image: $CURRENT_IMAGE")
        }
    }

}
