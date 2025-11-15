package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField

internal class NeqCondition<T>(field: MetaField<T>, value: T) :
    AbstractOneValueCondition<T>(field, value) {

    override val operator: String = "<>"

}
