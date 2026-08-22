package kolbasa.consumer.filter

import kolbasa.consumer.filter.Filter.between
import kolbasa.consumer.filter.Filter.like
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.greater
import kolbasa.consumer.filter.Filter.greaterEq
import kolbasa.consumer.filter.Filter.isNotNull
import kolbasa.consumer.filter.Filter.less
import kolbasa.consumer.filter.Filter.lessEq
import kolbasa.consumer.filter.Filter.neq
import kolbasa.consumer.filter.Filter.isNull
import kolbasa.consumer.filter.Filter.nativeSql
import kolbasa.consumer.filter.Filter.oneOf
import kolbasa.queue.meta.MetaField
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf

internal class FilterTest {

    private val strField = MetaField.ofString("str_value")

    @Test
    fun testEq() {
        assertInstanceOf<EqCondition<*>>(strField eq "local")
    }

    @Test
    fun testNeq() {
        assertInstanceOf<NeqCondition<*>>(strField neq "local")
    }

    @Test
    fun testGreater() {
        assertInstanceOf<GreaterThanCondition<*>>(strField greater "local")
    }

    @Test
    fun testGreaterEq() {
        assertInstanceOf<GreaterThanOrEqCondition<*>>(strField greaterEq "local")
    }

    @Test
    fun testLess() {
        assertInstanceOf<LessThanCondition<*>>(strField less "local")
    }

    @Test
    fun testLessEq() {
        assertInstanceOf<LessThanOrEqCondition<*>>(strField lessEq "local")
    }

    @Test
    fun testBetween() {
        assertInstanceOf<BetweenCondition<*>>(strField between "a" and "b")
    }

    @Test
    fun testLike() {
        assertInstanceOf<LikeCondition>(strField like "asd")
    }

    @Test
    fun testIsNull() {
        assertInstanceOf<IsNullCondition>(isNull(strField))
    }

    @Test
    fun testIsNotNull() {
        assertInstanceOf<IsNotNullCondition>(isNotNull(strField))
    }

    @Test
    fun testOneOf() {
        assertInstanceOf<OneOfCondition<*>>(strField oneOf listOf("local"))
    }

    @Test
    fun testNativeSql() {
        assertInstanceOf<NativeSqlCondition>(nativeSql("{0} like '%asd%'", strField))
    }
}
