package kolbasa.test

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.testcontainers.postgresql.PostgreSQLContainer
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

@Tag("unit-db")
abstract class AbstractPostgreSQLTest {

    // Every test gets its own database, so tests stay isolated while sharing the container
    // We CAN NOT USE default pgContainer.databaseName and pgContainer.jdbcUrl because it points to the default database,
    // not to the database of this test
    protected lateinit var databaseName: String
    protected val databaseUrl: String
        get() = "jdbc:postgresql://${pgContainer.host}:${pgContainer.firstMappedPort}/$databaseName"

    protected lateinit var dataSource: DataSource
    protected lateinit var dataSourceFirstSchema: DataSource
    protected lateinit var dataSourceSecondSchema: DataSource

    @BeforeEach
    fun init() {
        // Create the test database. It is never dropped: it goes away with the container when the JVM exits.
        // createConnection() retries for up to 2 minutes, so a container that logs "ready" a moment before its port
        // answers doesn't fail the test
        databaseName = "test_${DATABASE_COUNTER.incrementAndGet()}"
        pgContainer.createConnection("").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("create database $databaseName")
            }
        }

        // Init dataSource, public schema
        dataSource = HikariDataSource().apply {
            jdbcUrl = databaseUrl
            username = pgContainer.username
            password = pgContainer.password
            minimumIdle = 1
        }

        // Init dataSource, first schema
        dataSourceFirstSchema = HikariDataSource().apply {
            jdbcUrl = databaseUrl
            username = pgContainer.username
            password = pgContainer.password
            minimumIdle = 1
            schema = FIRST_SCHEMA_NAME
        }

        // Init dataSource, second schema
        dataSourceSecondSchema = HikariDataSource().apply {
            jdbcUrl = databaseUrl
            username = pgContainer.username
            password = pgContainer.password
            minimumIdle = 1
            schema = SECOND_SCHEMA_NAME
        }

        // Create all schemas
        dataSource.execute(listOf("create schema $FIRST_SCHEMA_NAME", "create schema $SECOND_SCHEMA_NAME"))

        // Insert test data for all schemas, if any
        dataSource.execute(generateTestData())
        dataSourceFirstSchema.execute(generateTestDataFirstSchema())
        dataSourceSecondSchema.execute(generateTestDataSecondSchema())
    }

    @AfterEach
    fun shutdown() {
        (dataSource as HikariDataSource).close()
        (dataSourceFirstSchema as HikariDataSource).close()
        (dataSourceSecondSchema as HikariDataSource).close()
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

        // All tests in this JVM share one container. Its own jdbcUrl and databaseName point to the default database,
        // not to the database of this test: use databaseUrl and databaseName instead.
        // Started on first use and never stopped by hand: Testcontainers removes it when the JVM exits
        val pgContainer: PostgreSQLContainer by lazy {
            // fsync=off is PostgreSQLContainer's own default, withCommand() replaces it, so it is repeated here.
            // Tests run in parallel and share the server, and each one opens up to 30 connections (3 pools of 10)
            PostgreSQLContainer(POSTGRESQL_IMAGE)
                .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=3000")
                .also { container ->
                    container.start()
                }
        }

        private val DATABASE_COUNTER = AtomicInteger()
    }

}

private fun DataSource.execute(statements: List<String>) = connection.use { connection ->
    connection.createStatement().use { statement -> statements.forEach(statement::execute) }
}
