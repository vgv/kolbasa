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
abstract class AbstractPostgresTest {

    @Container
    protected val pgContainer = PostgreSQLContainer(CURRENT_IMAGE)

    protected lateinit var dataSource: DataSource;

    @BeforeEach
    fun init() {
        // Start PG container
        pgContainer.start()

        // Init simple dataSource
        dataSource = HikariDataSource().apply {
            jdbcUrl = pgContainer.jdbcUrl
            username = pgContainer.username
            password = pgContainer.password
        }

        // Insert test data, if any
        // execute all statements
        dataSource.connection.use { connection ->
            connection.autoCommit = true // separate transaction for each statement
            connection.useStatement { statement ->
                generateTestData().forEach(statement::execute)
            }
        }
    }

    @AfterEach
    fun shutdown() {
        pgContainer.stop()
    }

    protected open fun generateTestData(): List<String> {
        return emptyList()
    }

    private companion object {
        private val POSTGRES_IMAGES = setOf(
            "postgres:11.20-alpine",
            "postgres:12.15-alpine",
            "postgres:13.11-alpine",
            "postgres:14.8-alpine",
            "postgres:15.3-alpine"
        )

        val CURRENT_IMAGE = POSTGRES_IMAGES.random()

        init {
            println("PostgreSQL docker image: $CURRENT_IMAGE")
        }
    }

}
