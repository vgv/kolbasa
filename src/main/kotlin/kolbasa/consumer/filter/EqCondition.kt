package kolbasa.consumer.filter

internal class EqCondition<M : Any, T>(fieldName: String, value: T) :
    AbstractOneValueCondition<M, T>(fieldName, value) {

    override val operator = "="

}
