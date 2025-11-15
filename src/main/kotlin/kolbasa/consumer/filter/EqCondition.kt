package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField

internal class EqCondition<T>(field: MetaField<T>, value: T) :
    AbstractOneValueCondition<T>(field, value) {

    override val operator = "="

}
