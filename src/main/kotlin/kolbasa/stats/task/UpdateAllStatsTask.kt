package kolbasa.stats.task

import kolbasa.Kolbasa
import kolbasa.stats.GlobalStats

internal class UpdateAllStatsTask : AbstractDumpStatsTask() {

    override fun dumpsToUpdate() = GlobalStats.dumpAndReset(onlyRealtimeDumps = false)

    override fun lockId(): Long = Kolbasa.statsConfig.allDumpLockId

    override fun reschedulingInterval() = Kolbasa.statsConfig.allDumpInterval

}
