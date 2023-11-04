package kolbasa.consumer.filter

internal class LikeCondition<Meta : Any>(fieldName: String, value: String) :
    AbstractOneValueCondition<Meta, String>(fieldName, value) {

    override val operator = " like "

}
