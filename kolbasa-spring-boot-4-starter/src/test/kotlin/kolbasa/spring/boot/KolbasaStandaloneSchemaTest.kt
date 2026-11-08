package kolbasa.spring.boot

import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.test.AbstractPostgreSQLTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import java.util.function.Supplier
import javax.sql.DataSource

class KolbasaStandaloneSchemaTest : AbstractPostgreSQLTest() {

    private val settings = KolbasaStandaloneSettings(listOf(Queue.of("test", PredefinedDataTypes.String)))

    // The context closes the DataSource when it stops, so every check runs inside run {}
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KolbasaAutoConfiguration::class.java))
        .withBean(DataSource::class.java, { dataSource })

    @Test
    fun testStandaloneSettings_CreatesTables() {
        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .run { context ->
                assertThat(context).hasNotFailed()
                assertTrue("q_test" in tables())
            }
    }

    @Test
    fun testBrokenTable_StartupFails() {
        // A table with the queue's name, but not a queue table. Kolbasa leaves it as is, and the indexes on its
        // missing columns fail on every try
        execute("create table q_test(x int)")

        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .run { context ->
                assertThat(context).hasFailed()
                val rootCause = generateSequence(context.startupFailure) { it.cause }.last()
                assertTrue(rootCause is IllegalStateException)
                assertTrue("create index" in rootCause.message.orEmpty())
            }
    }

    @Test
    fun testSchemaDisabled_NoTables() {
        contextRunner
            .withBean(KolbasaStandaloneSettings::class.java, { settings })
            .withPropertyValues("kolbasa.standalone.schema.enabled=false")
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context).doesNotHaveBean(KolbasaStandaloneSchemaInitializer::class.java)
                assertEquals(emptySet<String>(), tables())
            }
    }

    @Test
    fun testNoStandaloneSettings_DatabaseNotTouched() {
        contextRunner.run { context ->
            assertThat(context).hasNotFailed()
            assertThat(context).doesNotHaveBean(KolbasaStandaloneSchemaInitializer::class.java)
            assertEquals(emptySet<String>(), tables())
        }
    }

    private fun execute(sql: String) = dataSource.connection.use { connection ->
        connection.createStatement().use { statement -> statement.execute(sql) }
    }

    private fun tables(): Set<String> = dataSource.connection.use { connection ->
        connection.createStatement().use { statement ->
            val sql = "select table_name from information_schema.tables where table_schema = 'public'"
            statement.executeQuery(sql).use { resultSet ->
                buildSet { while (resultSet.next()) add(resultSet.getString(1)) }
            }
        }
    }
}
