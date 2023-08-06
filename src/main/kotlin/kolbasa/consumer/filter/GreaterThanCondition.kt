package kolbasa.consumer.filter

internal class GreaterThanCondition<Meta : Any, T>(fieldName: String, value: T) :
    AbstractOneValueCondition<Meta, T>(fieldName, value) {

    override val operator: String = ">"

}
