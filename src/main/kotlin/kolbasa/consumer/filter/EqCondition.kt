package kolbasa.consumer.filter

internal class EqCondition<Meta : Any, T>(fieldName: String, value: T) :
    AbstractOneValueCondition<Meta, T>(fieldName, value) {

    override val operator = "="

}
