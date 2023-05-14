package kolbasa.stats.task

import kolbasa.Kolbasa
import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.Lock
import kolbasa.stats.Measure
import org.slf4j.LoggerFactory
import javax.sql.DataSource

internal class DeleteOutdatedMeasuresTask : AbstractReschedulingTask() {

    override fun doWork() {
        val measures = Measure.values().toList()
        val lockId = Kolbasa.statsConfig.deleteOutdatedMeasuresLockId

        Kolbasa.statsConfig.dataSources.forEach { dataSource ->
            Lock.tryRunExclusive(dataSource, lockId) {
                deleteOutdatedMeasures(dataSource, measures)
            }
        }
    }

    override fun reschedulingInterval() = Kolbasa.statsConfig.deleteOutdatedMeasuresInterval

    private fun deleteOutdatedMeasures(dataSource: DataSource, measures: List<Measure>) {
        try {
            val deletedRows = dataSource.usePreparedStatement(StatsSchemaHelpers.STATS_INTERNAL_DELETE_OUTDATED_QUERY) { ps ->
                measures.forEach { measure ->
                    ps.setString(1, measure.measureName)
                    ps.setLong(2, measure.oldestValidTick())
                    ps.addBatch()
                }

                ps.executeBatch()
            }

            log.debug("Delete $deletedRows outdated measures")
        } catch (e: Exception) {
            log.error("Can't delete outdated measures", e)
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(DeleteOutdatedMeasuresTask::class.qualifiedName)
    }
}
