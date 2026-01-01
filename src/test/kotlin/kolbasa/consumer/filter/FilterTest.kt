package kolbasa.consumer.filter

import kolbasa.consumer.filter.Filter.and
import kolbasa.consumer.filter.Filter.between
import kolbasa.consumer.filter.Filter.like
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.greater
import kolbasa.consumer.filter.Filter.greaterEq
import kolbasa.consumer.filter.Filter.`in`
import kolbasa.consumer.filter.Filter.isNotNull
import kolbasa.consumer.filter.Filter.less
import kolbasa.consumer.filter.Filter.lessEq
import kolbasa.consumer.filter.Filter.neq
import kolbasa.consumer.filter.Filter.or
import kolbasa.consumer.filter.Filter.isNull
import kolbasa.consumer.filter.Filter.nativeSql
import kolbasa.consumer.filter.Filter.not
import kolbasa.queue.meta.MetaField
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf

internal class FilterTest {

    @Test
    fun testEq() {
        assertInstanceOf<EqCondition<*>>(STR_FIELD eq "local")
    }

    @Test
    fun testNeq() {
        assertInstanceOf<NeqCondition<*>>(STR_FIELD neq "local")
    }

    @Test
    fun testGreater() {
        assertInstanceOf<GreaterThanCondition<*>>(STR_FIELD greater "local")
    }

    @Test
    fun testGreaterEq() {
        assertInstanceOf<GreaterThanOrEqCondition< *>>(STR_FIELD greaterEq "local")
    }

    @Test
    fun testLess() {
        assertInstanceOf<LessThanCondition<*>>(STR_FIELD less "local")
    }

    @Test
    fun testLessEq() {
        assertInstanceOf<LessThanOrEqCondition<*>>(STR_FIELD lessEq "local")
    }

    @Test
    fun testBetween() {
        assertInstanceOf<BetweenCondition<*>>(STR_FIELD between Pair("a", "b"))
    }

    @Test
    fun testLike() {
        assertInstanceOf<LikeCondition>(STR_FIELD like "asd")
    }

    @Test
    fun testAnd() {
        assertInstanceOf<AndCondition>((STR_FIELD eq "123") and (STR_FIELD neq "2234"))
    }

    @Test
    fun testOr() {
        assertInstanceOf<OrCondition>((STR_FIELD eq "123") or (STR_FIELD neq "2234"))
    }

    @Test
    fun testIsNull() {
        assertInstanceOf<IsNullCondition>(isNull(STR_FIELD))
    }

    @Test
    fun testIsNotNull() {
        assertInstanceOf<IsNotNullCondition>(isNotNull(STR_FIELD))
    }

    @Test
    fun testNot() {
        assertInstanceOf<NotCondition>(not(STR_FIELD eq "123"))
    }

    @Test
    fun testIn() {
        assertInstanceOf<InCondition<*>>(STR_FIELD `in` listOf("local"))
    }

    @Test
    fun testNativeSql() {
        assertInstanceOf<NativeSqlCondition>(nativeSql("{0} like '%asd%'", STR_FIELD))
    }
}

private val STR_FIELD = MetaField.string("str_value")
