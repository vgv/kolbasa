package kolbasa.stats

internal data class MeasureDump(
    val measure: Measure,
    val oldestValidTick: Long,
    val data: Map<Long, Long>
)

internal data class QueueDump(
    val queue: String,
    val measures: List<MeasureDump>
)
