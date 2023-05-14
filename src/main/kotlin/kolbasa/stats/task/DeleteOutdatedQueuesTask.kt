package kolbasa.stats.task

import kolbasa.Kolbasa
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.pg.Lock
import org.slf4j.LoggerFactory
import javax.sql.DataSource

class DeleteOutdatedQueuesTask : AbstractReschedulingTask() {

    override fun doWork() {
        val lockId = Kolbasa.statsConfig.deleteOutdatedQueuesLockId

        Kolbasa.statsConfig.dataSources.forEach { dataSource ->
            Lock.tryRunExclusive(dataSource, lockId) {
                deleteOutdated(dataSource)
            }
        }
    }

    override fun reschedulingInterval() = Kolbasa.statsConfig.deleteOutdatedQueuesInterval

    private fun deleteOutdated(dataSource: DataSource) {
        try {
            val queue = StatsSchemaHelpers.generateStatsDeleteOutdatedQueuesQuery(DAYS)
            val deletedRows = dataSource.useStatement { statement ->
                statement.executeUpdate(queue)
            }

            log.debug("Delete $deletedRows outdated queues")
        } catch (e: Exception) {
            log.error("Can't delete outdated queues", e)
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(DeleteOutdatedQueuesTask::class.qualifiedName)

        private const val DAYS = 365 // I don't see any reasons to make this value tunable
    }
}
