package kolbasa.telemetry.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer

/**
 * Meters with one name and the same tag keys, the way Prometheus sees one metric with its labels.
 *
 * [withTags] gives the meter for one set of tag values. Ask once for every set and keep the meter: looking it up in the
 * registry on every call costs more than the call to the meter itself.
 */
internal abstract class MeterFamily<M>(
    private val registry: () -> MeterRegistry,
    protected val name: String,
    protected val description: String,
    private val tagKeys: List<String>
) {

    fun withTags(vararg tagValues: String): M {
        require(tagValues.size == tagKeys.size) {
            "Meter $name has tags $tagKeys, but got ${tagValues.size} values"
        }

        val tags = Tags.of(tagKeys.zip(tagValues).map { (key, value) -> Tag.of(key, value) })
        return register(registry(), tags)
    }

    protected abstract fun register(registry: MeterRegistry, tags: Tags): M
}

internal class CounterFamily(
    registry: () -> MeterRegistry,
    name: String,
    description: String,
    tagKeys: List<String>,
    private val baseUnit: String? = null
) : MeterFamily<Counter>(registry, name, description, tagKeys) {

    override fun register(registry: MeterRegistry, tags: Tags): Counter {
        return Counter.builder(name)
            .description(description)
            .baseUnit(baseUnit)
            .tags(tags)
            .register(registry)
    }
}

/**
 * Durations of calls, with the same buckets for every family, see [Const.callsDurationHistogramBuckets].
 */
internal class TimerFamily(
    registry: () -> MeterRegistry,
    name: String,
    description: String,
    tagKeys: List<String>
) : MeterFamily<Timer>(registry, name, description, tagKeys) {

    override fun register(registry: MeterRegistry, tags: Tags): Timer {
        return Timer.builder(name)
            .description(description)
            .serviceLevelObjectives(*Const.callsDurationHistogramBuckets())
            .tags(tags)
            .register(registry)
    }
}

/**
 * Gauges whose value kolbasa sets itself, see [GaugeValue].
 *
 * Ask only once for every set of tag values. A registry keeps the first gauge with a name and tags, so the value of a
 * second one would go nowhere.
 */
internal class GaugeFamily(
    registry: () -> MeterRegistry,
    name: String,
    description: String,
    tagKeys: List<String>,
    private val baseUnit: String? = null
) : MeterFamily<GaugeValue>(registry, name, description, tagKeys) {

    override fun register(registry: MeterRegistry, tags: Tags): GaugeValue {
        val value = GaugeValue()

        // A strong reference: by default a gauge holds its object weakly, and the value would be collected
        Gauge.builder(name, value, GaugeValue::get)
            .description(description)
            .baseUnit(baseUnit)
            .tags(tags)
            .strongReference(true)
            .register(registry)

        return value
    }
}

/**
 * The value of one gauge.
 *
 * Micrometer reads a gauge when the metrics are collected. kolbasa can't read a queue at that moment, it has no
 * connection then. So it reads the queue during a call and keeps the result here, for Micrometer to pick up.
 */
internal class GaugeValue {

    @Volatile
    private var value: Double = 0.0

    fun get(): Double = value

    fun set(value: Double) {
        this.value = value
    }
}
