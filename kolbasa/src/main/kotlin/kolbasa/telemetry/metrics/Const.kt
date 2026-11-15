package kolbasa.telemetry.metrics

import java.time.Duration

internal object Const {

    fun callsDurationHistogramBuckets(): Array<Duration> = arrayOf(
        Duration.ofMillis(1),
        Duration.ofMillis(2),
        Duration.ofMillis(5),
        Duration.ofMillis(10),
        Duration.ofMillis(20),
        Duration.ofMillis(50),
        Duration.ofMillis(100),
        Duration.ofMillis(200),
        Duration.ofMillis(500),
        Duration.ofSeconds(1),
        Duration.ofSeconds(2),
        Duration.ofSeconds(5),
        Duration.ofSeconds(10),
        Duration.ofSeconds(30)
    )

}
