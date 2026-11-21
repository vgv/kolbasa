package kolbasa.spring.boot

import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import org.springframework.beans.factory.InitializingBean
import javax.sql.DataSource

/**
 * Creates or updates the tables of [KolbasaStandaloneSettings.queues] in the application's `DataSource` at startup.
 *
 * The starter creates this bean when there is a [KolbasaStandaloneSettings] bean and
 * `kolbasa.standalone.schema.enabled` is not `false`. The tables are ready once the bean is created, so a bean that
 * depends on it can use the queues at once.
 *
 * A failed statement is tried again: `kolbasa.standalone.schema.startup-attempts` times in all, 5 by default, with
 * `kolbasa.standalone.schema.startup-retry-delay` before every next try, 1 second by default. Two instances that start
 * at the same time can race on the same statement, and the next try finds the work done. If statements still fail, the
 * application doesn't start, and the exception lists them.
 */
class KolbasaStandaloneSchemaInitializer internal constructor(
    private val dataSource: DataSource,
    private val queues: List<Queue<*>>,
    private val startup: SchemaStartupAttempts
) : InitializingBean {

    override fun afterPropertiesSet() {
        val result = startup.run(
            { SchemaHelpers.createOrUpdateQueues(dataSource, queues) },
            { it.failedStatements == 0 }
        )

        check(result.failedStatements == 0) {
            val failed = result.failedTableStatements + result.failedIndexStatements
            "Can't create the tables of kolbasa queues after ${startup.attempts} attempts, see " +
                "${SchemaStartupAttempts.STANDALONE_SCHEMA_PREFIX}.startup-attempts:\n" +
                failed.joinToString("\n") { "${it.statement}\n  -> ${it.error.message}" }
        }
    }
}
