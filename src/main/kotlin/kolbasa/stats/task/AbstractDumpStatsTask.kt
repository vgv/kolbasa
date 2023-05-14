package kolbasa.stats.task

import kolbasa.Kolbasa
import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.pg.Lock
import kolbasa.stats.QueueDump
import org.slf4j.LoggerFactory
import javax.sql.DataSource

internal abstract class AbstractDumpStatsTask : AbstractReschedulingTask() {

    abstract fun dumpsToUpdate(): List<QueueDump>

    abstract fun lockId(): Long

    override fun doWork() {
        val lockId = lockId()
        val dumps = dumpsToUpdate()

        Kolbasa.statsConfig.dataSources.forEach { dataSource ->
            updateStats(dataSource, lockId, dumps)
        }
    }

    private fun updateStats(dataSource: DataSource, lockId: Long, dumps: List<QueueDump>) {
        try {
            // All clients have to insert new measures
            updateMeasures(dataSource, dumps)


            // but only one has to recalculate stats at the same time
            Lock.tryRunExclusive(dataSource, lockId) {
                calculateStats(dataSource, dumps)
            }
        } catch (e: Exception) {
            log.error("Can't update and recalculate stats", e)
        }
    }

    private fun updateMeasures(dataSource: DataSource, dumps: List<QueueDump>) {
        // Update ALL measures in one batch
        dataSource.usePreparedStatement(StatsSchemaHelpers.STATS_INTERNAL_INSERT_QUERY) { ps ->
            dumps.forEach { queueDump ->
                queueDump.measures.forEach { measureDump ->
                    measureDump.data.forEach { (tick, value) ->
                        ps.setString(1, queueDump.queue)
                        ps.setString(2, measureDump.measure.measureName)
                        ps.setLong(3, tick)
                        ps.setLong(4, value)
                        ps.addBatch()
                    }
                }
            }

            ps.executeBatch()
        }
    }

    private fun calculateStats(dataSource: DataSource, dumps: List<QueueDump>) {
        dumps.forEach { queueDump ->
            val statsData = mutableListOf<Long>()

            dataSource.useStatement { statement ->
                statement.executeQuery(StatsSchemaHelpers.generateAggregateQuery(queueDump)).use { resultSet ->
                    if (resultSet.next()) {
                        var column = 1
                        queueDump.measures.forEach { _ ->
                            statsData += resultSet.getLong(column++)
                        }
                    }
                }
            }

            dataSource.usePreparedStatement(StatsSchemaHelpers.generateStatsInsertQuery(queueDump)) { ps ->
                var column = 1

                ps.setString(column++, queueDump.queue)
                statsData.forEach {
                    ps.setLong(column++, it)
                }

                ps.executeUpdate()
            }
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(AbstractDumpStatsTask::class.qualifiedName)
    }
}
