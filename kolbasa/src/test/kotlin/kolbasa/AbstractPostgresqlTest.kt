package kolbasa

import com.zaxxer.hikari.HikariDataSource
import kolbasa.utils.JdbcHelpers.useConnectionWithAutocommit
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.postgresql.PostgreSQLContainer
import javax.sql.DataSource

@Tag("unit-db")
abstract class AbstractPostgresqlTest {

    @Container
    protected val pgContainer = PostgreSQLContainer(RANDOM_POSTGRES_IMAGE)

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
        dataSource.useConnectionWithAutocommit { connection ->
            connection.useStatement { statement ->
                generateTestData().forEach(statement::execute)
            }
        }
        // execute all statements in a separate transaction for each statement
        dataSourceFirstSchema.useConnectionWithAutocommit { connection ->
            connection.useStatement { statement ->
                generateTestDataFirstSchema().forEach(statement::execute)
            }
        }
        // execute all statements in a separate transaction for each statement
        dataSourceSecondSchema.useConnectionWithAutocommit { connection ->
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

        // All PG images to run tests against. Loaded from src/test/resources/postgresql-test-images.txt
        // One image per line, blank lines and '#' comments are ignored.
        private val POSTGRES_IMAGES: Set<String> =
            requireNotNull(AbstractPostgresqlTest::class.java.getResourceAsStream("/postgresql-test-images.txt")) {
                "postgresql-test-images.txt not found on the test classpath"
            }.bufferedReader().useLines { lines ->
                lines.map(String::trim)
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .toSet()
            }.also {
                require(it.isNotEmpty()) { "postgresql-test-images.txt parsed to 0 images" }
            }

        // The latest patch of each major version (e.g. 14.23, 15.18, 16.14, 17.10, 18.4).
        private val LATEST_POSTGRES_IMAGES: List<String> = POSTGRES_IMAGES
            .groupBy { pgVersion(it).first } // group by major
            .map { (_, images) -> images.maxBy { pgVersion(it).second } } // highest minor

        // Chosen once per JVM: an explicit override (used by the per-version Gradle tasks), otherwise a
        // random latest-per-major image. Sampling only the latest patches keeps the local Docker image
        // cache bounded to ~one per major, while still rotating coverage across majors run-to-run.
        val RANDOM_POSTGRES_IMAGE: String =
            System.getProperty("kolbasa.test.postgresql.image") ?: LATEST_POSTGRES_IMAGES.random()

        // Newest image by parsed version (major, then minor), so it does not depend on file ordering.
        val NEWEST_POSTGRES_IMAGE: String =
            POSTGRES_IMAGES.maxWith(compareBy({ pgVersion(it).first }, { pgVersion(it).second }))

        // "postgres:16.4-alpine" -> (16, 4)
        private fun pgVersion(image: String): Pair<Int, Int> {
            val (major, minor) = image.substringAfter(':').substringBefore('-').split('.').map(String::toInt)
            return major to minor
        }

        init {
            println("PostgreSQL docker image: $RANDOM_POSTGRES_IMAGE")
        }
    }

}
