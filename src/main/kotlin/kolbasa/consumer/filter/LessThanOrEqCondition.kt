package kolbasa.consumer.filter

internal class LessThanOrEqCondition<M : Any, T>(fieldName: String, value: T) :
    AbstractOneValueCondition<M, T>(fieldName, value) {

    override val operator: String = "<="

}
