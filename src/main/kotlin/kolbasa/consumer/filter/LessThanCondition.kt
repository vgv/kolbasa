package kolbasa.consumer.filter

internal class LessThanCondition<M : Any, T>(fieldName: String, value: T) :
    AbstractOneValueCondition<M, T>(fieldName, value) {

    override val operator: String = "<"

}
