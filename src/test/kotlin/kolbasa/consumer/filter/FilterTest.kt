package kolbasa.consumer.filter

import kolbasa.consumer.filter.Filter.between
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.greater
import kolbasa.consumer.filter.Filter.greaterEq
import kolbasa.consumer.filter.Filter.isNotNull
import kolbasa.consumer.filter.Filter.isNull
import kolbasa.consumer.filter.Filter.less
import kolbasa.consumer.filter.Filter.lessEq
import kolbasa.consumer.filter.Filter.like
import kolbasa.consumer.filter.Filter.nativeSql
import kolbasa.consumer.filter.Filter.neq
import kolbasa.consumer.filter.Filter.oneOf
import kolbasa.queue.meta.MetaField
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class FilterTest {

    private val strField = MetaField.ofString("str_value")

    @Test
    fun testEq() {
        val expected = EqCondition(strField, "local")
        val actual = strField eq "local"

        assertEquals(expected, actual)
    }

    @Test
    fun testNeq() {
        val expected = NeqCondition(strField, "local")
        val actual = strField neq "local"

        assertEquals(expected, actual)
    }

    @Test
    fun testGreater() {
        val expected = GreaterThanCondition(strField, "local")
        val actual = strField greater "local"

        assertEquals(expected, actual)
    }

    @Test
    fun testGreaterEq() {
        val expected = GreaterThanOrEqCondition(strField, "local")
        val actual = strField greaterEq "local"

        assertEquals(expected, actual)
    }

    @Test
    fun testLess() {
        val expected = LessThanCondition(strField, "local")
        val actual = strField less "local"

        assertEquals(expected, actual)
    }

    @Test
    fun testLessEq() {
        val expected = LessThanOrEqCondition(strField, "local")
        val actual = strField lessEq "local"

        assertEquals(expected, actual)
    }

    @Test
    fun testBetween() {
        val expected = BetweenCondition(strField, "a", "b")
        val actual = strField between "a" and "b"

        assertEquals(expected, actual)
    }

    @Test
    fun testLike() {
        val expected = LikeCondition(strField, "asd")
        val actual = strField like "asd"

        assertEquals(expected, actual)
    }

    @Test
    fun testIsNull() {
        val expected = IsNullCondition(strField)
        val actual = isNull(strField)

        assertEquals(expected, actual)
    }

    @Test
    fun testIsNotNull() {
        val expected = IsNotNullCondition(strField)
        val actual = isNotNull(strField)

        assertEquals(expected, actual)
    }

    @Test
    fun testOneOf() {
        val expected = OneOfCondition(strField, listOf("a", "b", "c"))
        val actual = strField oneOf listOf("a", "b", "c")

        assertEquals(expected, actual)
    }

    @Test
    fun testNativeSql() {
        val expected = NativeSqlCondition("{0} ilike '%asd%'", listOf(strField))
        val actual = nativeSql("{0} ilike '%asd%'", strField)

        assertEquals(expected, actual)
    }
}
