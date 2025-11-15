package kolbasa.consumer.filter

import kolbasa.queue.meta.MetaField

internal class LikeCondition(field: MetaField<String>, value: String) :
    AbstractOneValueCondition<String>(field, value) {

    override val operator = "like"

}
