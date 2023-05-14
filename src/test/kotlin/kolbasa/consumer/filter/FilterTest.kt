package kolbasa.consumer.filter

import kolbasa.consumer.filter.Filter.and
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.greater
import kolbasa.consumer.filter.Filter.greaterEq
import kolbasa.consumer.filter.Filter.isNotNull
import kolbasa.consumer.filter.Filter.less
import kolbasa.consumer.filter.Filter.lessEq
import kolbasa.consumer.filter.Filter.neq
import kolbasa.consumer.filter.Filter.or
import kolbasa.consumer.filter.Filter.isNull
import kolbasa.consumer.filter.Filter.not
import org.junit.jupiter.api.Test
import kotlin.test.assertIs

internal class FilterTest {

    @Test
    fun testEq() {
        assertIs<EqCondition<*, *>>(TestMeta::strValue eq "local")
    }

    @Test
    fun testNeq() {
        assertIs<NeqCondition<*, *>>(TestMeta::strValue neq "local")
    }

    @Test
    fun testGreater() {
        assertIs<GreaterThanCondition<*, *>>(TestMeta::strValue greater "local")
    }

    @Test
    fun testGreaterEq() {
        assertIs<GreaterThanOrEqCondition<*, *>>(TestMeta::strValue greaterEq "local")
    }

    @Test
    fun testLess() {
        assertIs<LessThanCondition<*, *>>(TestMeta::strValue less "local")
    }

    @Test
    fun testLessEq() {
        assertIs<LessThanOrEqCondition<*, *>>(TestMeta::strValue lessEq "local")
    }

    @Test
    fun testAnd() {
        assertIs<AndCondition<*>>((TestMeta::strValue eq "123") and (TestMeta::strValue neq "2234"))
    }

    @Test
    fun testOr() {
        assertIs<OrCondition<*>>((TestMeta::strValue eq "123") or (TestMeta::strValue neq "2234"))
    }

    @Test
    fun tesIisNull() {
        assertIs<IsNullCondition<*>>(isNull(TestMeta::strValue))
    }

    @Test
    fun testIsNotNull() {
        assertIs<IsNotNullCondition<*>>(isNotNull(TestMeta::strValue))
    }

    @Test
    fun testNot() {
        assertIs<NotCondition<*>>(not(TestMeta::strValue eq "123"))
    }
}

private data class TestMeta(val strValue: String?)
