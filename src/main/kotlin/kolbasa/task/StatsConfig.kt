package kolbasa.task

import java.time.Duration
import javax.sql.DataSource

data class StatsConfig(
    val dataSources: List<DataSource> = emptyList(),

    val realtimeDumpInterval: Duration = Duration.ofSeconds(15),
    val allDumpInterval: Duration = Duration.ofMinutes(5),
    val deleteOutdatedMeasuresInterval: Duration = Duration.ofHours(1),
    val deleteOutdatedQueuesInterval: Duration = Duration.ofDays(1),

    val realtimeDumpLockId: Long = "realtime-dump".hashCode().toLong(),
    val allDumpLockId: Long = "all-dump".hashCode().toLong(),
    val deleteOutdatedMeasuresLockId: Long = "delete-outdated-measures".hashCode().toLong(),
    val deleteOutdatedQueuesLockId: Long = "delete-outdated-queues".hashCode().toLong()
)


