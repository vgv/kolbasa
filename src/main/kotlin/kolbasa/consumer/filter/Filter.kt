package kolbasa.consumer.filter

import kolbasa.consumer.JavaField
import kotlin.reflect.KFunction1
import kotlin.reflect.KProperty1

object Filter {

    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KProperty1<M, T?>.eq(value: T): Condition<M> {
        return EqCondition(this.name, value)
    }

    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KFunction1<M, T?>.eq(value: T): Condition<M> {
        return EqCondition(this.name, value)
    }

    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> JavaField<M, T?>.eq(value: T): Condition<M> {
        return EqCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KProperty1<M, T?>.neq(value: T): Condition<M> {
        return NeqCondition(this.name, value)
    }

    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KFunction1<M, T?>.neq(value: T): Condition<M> {
        return NeqCondition(this.name, value)
    }

    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> JavaField<M, T?>.neq(value: T): Condition<M> {
        return NeqCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KProperty1<M, T?>.greater(value: T): Condition<M> {
        return GreaterThanCondition(this.name, value)
    }

    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KFunction1<M, T?>.greater(value: T): Condition<M> {
        return GreaterThanCondition(this.name, value)
    }

    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> JavaField<M, T?>.greater(value: T): Condition<M> {
        return GreaterThanCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KProperty1<M, T?>.greaterEq(value: T): Condition<M> {
        return GreaterThanOrEqCondition(this.name, value)
    }
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KFunction1<M, T?>.greaterEq(value: T): Condition<M> {
        return GreaterThanOrEqCondition(this.name, value)
    }
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> JavaField<M, T?>.greaterEq(value: T): Condition<M> {
        return GreaterThanOrEqCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KProperty1<M, T?>.less(value: T): Condition<M> {
        return LessThanCondition(this.name, value)
    }
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KFunction1<M, T?>.less(value: T): Condition<M> {
        return LessThanCondition(this.name, value)
    }
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> JavaField<M, T?>.less(value: T): Condition<M> {
        return LessThanCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KProperty1<M, T?>.lessEq(value: T): Condition<M> {
        return LessThanOrEqCondition(this.name, value)
    }
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> KFunction1<M, T?>.lessEq(value: T): Condition<M> {
        return LessThanOrEqCondition(this.name, value)
    }
    @JvmStatic
    infix fun <M : Any, T : Comparable<T>> JavaField<M, T?>.lessEq(value: T): Condition<M> {
        return LessThanOrEqCondition(this.name, value)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <M : Any> Condition<M>.and(condition: Condition<M>): Condition<M> {
        return AndCondition(this, condition)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    infix fun <M : Any> Condition<M>.or(condition: Condition<M>): Condition<M> {
        return OrCondition(this, condition)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    fun <M : Any, T : Comparable<T>> isNull(property: KProperty1<M, T?>): Condition<M> {
        return IsNullCondition(property.name)
    }
    @JvmStatic
    fun <M : Any, T : Comparable<T>> isNull(property: KFunction1<M, T?>): Condition<M> {
        return IsNullCondition(property.name)
    }
    @JvmStatic
    fun <M : Any, T : Comparable<T>> isNull(property: JavaField<M, T?>): Condition<M> {
        return IsNullCondition(property.name)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    fun <M : Any, T : Comparable<T>> isNotNull(property: KProperty1<M, T?>): Condition<M> {
        return IsNotNullCondition(property.name)
    }
    @JvmStatic
    fun <M : Any, T : Comparable<T>> isNotNull(property: KFunction1<M, T?>): Condition<M> {
        return IsNotNullCondition(property.name)
    }
    @JvmStatic
    fun <M : Any, T : Comparable<T>> isNotNull(property: JavaField<M, T?>): Condition<M> {
        return IsNotNullCondition(property.name)
    }

    // -------------------------------------------------------------------------------------------
    @JvmStatic
    fun <M : Any> not(condition: Condition<M>): Condition<M> {
        return NotCondition(condition)
    }

}

