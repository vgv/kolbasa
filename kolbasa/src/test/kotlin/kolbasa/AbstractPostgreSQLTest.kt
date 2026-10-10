package kolbasa

import kolbasa.test.PostgreSQLImages

import com.zaxxer.hikari.HikariDataSource
import kolbasa.utils.JdbcHelpers.useStatement
import kolbasa.utils.JdbcHelpers.withAutoCommit
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.postgresql.PostgreSQLContainer
import javax.sql.DataSource

@Tag("unit-db")
abstract class AbstractPostgreSQLTest {

    @Container
    protected val pgContainer = PostgreSQLContainer(POSTGRESQL_IMAGE)

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
        // execute all statements in a separate transaction for each statement
        dataSource.withAutoCommit { connection ->
            connection.useStatement { statement ->
                generateTestData().forEach(statement::execute)
            }
        }
        // execute all statements in a separate transaction for each statement
        dataSourceFirstSchema.withAutoCommit { connection ->
            connection.useStatement { statement ->
                generateTestDataFirstSchema().forEach(statement::execute)
            }
        }
        // execute all statements in a separate transaction for each statement
        dataSourceSecondSchema.withAutoCommit { connection ->
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

        // Which PostgreSQL versions the project is tested against is decided in one place for the whole
        // repository - the :test-support module. See kolbasa.test.PostgreSQLImages.
        val POSTGRESQL_IMAGE: String = PostgreSQLImages.randomImage

        init {
            println("PostgreSQL docker image: $POSTGRESQL_IMAGE")
        }
    }

}
