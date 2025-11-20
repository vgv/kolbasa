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
import kotlin.test.assertIs

internal class FilterTest {

    @Test
    fun testEq() {
        assertIs<EqCondition<*>>(STR_FIELD eq "local")
    }

    @Test
    fun testNeq() {
        assertIs<NeqCondition<*>>(STR_FIELD neq "local")
    }

    @Test
    fun testGreater() {
        assertIs<GreaterThanCondition<*>>(STR_FIELD greater "local")
    }

    @Test
    fun testGreaterEq() {
        assertIs<GreaterThanOrEqCondition< *>>(STR_FIELD greaterEq "local")
    }

    @Test
    fun testLess() {
        assertIs<LessThanCondition<*>>(STR_FIELD less "local")
    }

    @Test
    fun testLessEq() {
        assertIs<LessThanOrEqCondition<*>>(STR_FIELD lessEq "local")
    }

    @Test
    fun testBetween() {
        assertIs<BetweenCondition<*>>(STR_FIELD between Pair("a", "b"))
    }

    @Test
    fun testLike() {
        assertIs<LikeCondition>(STR_FIELD like "asd")
    }

    @Test
    fun testAnd() {
        assertIs<AndCondition>((STR_FIELD eq "123") and (STR_FIELD neq "2234"))
    }

    @Test
    fun testOr() {
        assertIs<OrCondition>((STR_FIELD eq "123") or (STR_FIELD neq "2234"))
    }

    @Test
    fun testIsNull() {
        assertIs<IsNullCondition>(isNull(STR_FIELD))
    }

    @Test
    fun testIsNotNull() {
        assertIs<IsNotNullCondition>(isNotNull(STR_FIELD))
    }

    @Test
    fun testNot() {
        assertIs<NotCondition>(not(STR_FIELD eq "123"))
    }

    @Test
    fun testIn() {
        assertIs<InCondition<*>>(STR_FIELD `in` listOf("local"))
    }

    @Test
    fun testNativeSql() {
        assertIs<NativeSqlCondition>(nativeSql("{0} like '%asd%'", STR_FIELD))
    }
}

private val STR_FIELD = MetaField.string("str_value")
