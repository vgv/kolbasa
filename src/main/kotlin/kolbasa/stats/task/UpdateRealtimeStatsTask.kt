package kolbasa.stats.task

import kolbasa.Kolbasa
import kolbasa.stats.GlobalStats
import org.slf4j.LoggerFactory

internal class UpdateRealtimeStatsTask : AbstractDumpStatsTask() {

    override fun dumpsToUpdate() = GlobalStats.dumpAndReset(onlyRealtimeDumps = true)

    override fun lockId() = Kolbasa.statsConfig.realtimeDumpLockId

    override fun reschedulingInterval() = Kolbasa.statsConfig.realtimeDumpInterval

}
