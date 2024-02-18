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
    protected val pgContainer = PostgreSQLContainer(CURRENT_POSTGRES_IMAGE.dockerImage)

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

        data class Postgres(val dockerImage: String, val modernVacuumStats: Boolean)

        // All PG images to run tests
        // Choose random image at every run
        private val POSTGRES_IMAGES = setOf(
            Postgres("postgres:10.23-alpine", false),
            Postgres("postgres:11.22-alpine", false),
            Postgres("postgres:12.18-alpine", false),
            Postgres("postgres:13.14-alpine", false),
            Postgres("postgres:14.11-alpine", true),
            Postgres("postgres:15.6-alpine", true),
            Postgres("postgres:16.2-alpine", true)
        )

        val CURRENT_POSTGRES_IMAGE = POSTGRES_IMAGES.random()

        init {
            println("PostgreSQL docker image: ${CURRENT_POSTGRES_IMAGE.dockerImage}")
        }
    }

}
