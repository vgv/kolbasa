package kolbasa.stats

import java.time.Duration

internal enum class MeasureType {
    SEND_CALLS,
    SEND_BYTES,
    RECEIVE_CALLS,
    RECEIVE_BYTES
}

internal enum class Measure(
    val measureName: String,
    val duration: Duration,
    val measureType: MeasureType,
    val realtime: Boolean
) {
    SEND_1M("send_1m", Duration.ofMinutes(1), MeasureType.SEND_CALLS, true),
    SEND_5M("send_5m", Duration.ofMinutes(5), MeasureType.SEND_CALLS, true),
    SEND_15M("send_15m", Duration.ofMinutes(15), MeasureType.SEND_CALLS, true),
    SEND_1H("send_1h", Duration.ofHours(1), MeasureType.SEND_CALLS, false),
    SEND_6H("send_6h", Duration.ofHours(6), MeasureType.SEND_CALLS, false),
    SEND_1D("send_1d", Duration.ofDays(1), MeasureType.SEND_CALLS, false),

    SEND_1M_BYTES("send_1m_bytes", Duration.ofMinutes(1), MeasureType.SEND_BYTES, true),
    SEND_5M_BYTES("send_5m_bytes", Duration.ofMinutes(5), MeasureType.SEND_BYTES, true),
    SEND_15M_BYTES("send_15m_bytes", Duration.ofMinutes(15), MeasureType.SEND_BYTES, true),
    SEND_1H_BYTES("send_1h_bytes", Duration.ofHours(1), MeasureType.SEND_BYTES, false),
    SEND_6H_BYTES("send_6h_bytes", Duration.ofHours(6), MeasureType.SEND_BYTES, false),
    SEND_1D_BYTES("send_1d_bytes", Duration.ofDays(1), MeasureType.SEND_BYTES, false),

    RECEIVE_1M("receive_1m", Duration.ofMinutes(1), MeasureType.RECEIVE_CALLS, true),
    RECEIVE_5M("receive_5m", Duration.ofMinutes(5), MeasureType.RECEIVE_CALLS, true),
    RECEIVE_15M("receive_15m", Duration.ofMinutes(15), MeasureType.RECEIVE_CALLS, true),
    RECEIVE_1H("receive_1h", Duration.ofHours(1), MeasureType.RECEIVE_CALLS, false),
    RECEIVE_6H("receive_6h", Duration.ofHours(6), MeasureType.RECEIVE_CALLS, false),
    RECEIVE_1D("receive_1d", Duration.ofDays(1), MeasureType.RECEIVE_CALLS, false),

    RECEIVE_1M_BYTES("receive_1m_bytes", Duration.ofMinutes(1), MeasureType.RECEIVE_BYTES, true),
    RECEIVE_5M_BYTES("receive_5m_bytes", Duration.ofMinutes(5), MeasureType.RECEIVE_BYTES, true),
    RECEIVE_15M_BYTES("receive_15m_bytes", Duration.ofMinutes(15), MeasureType.RECEIVE_BYTES, true),
    RECEIVE_1H_BYTES("receive_1h_bytes", Duration.ofHours(1), MeasureType.RECEIVE_BYTES, false),
    RECEIVE_6H_BYTES("receive_6h_bytes", Duration.ofHours(6), MeasureType.RECEIVE_BYTES, false),
    RECEIVE_1D_BYTES("receive_1d_bytes", Duration.ofDays(1), MeasureType.RECEIVE_BYTES, false);

    private val oneTick = duration.toMillis() / StatsConst.SLIDING_WINDOW_PRECISION

    fun currentTick() = System.currentTimeMillis() / oneTick
    fun oldestValidTick() = (System.currentTimeMillis() - duration.toMillis()) / oneTick

    init {
        check(duration.toMillisPart() == 0) {
            "You can't use durations with fractional seconds"
        }
    }

}
