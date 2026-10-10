package kolbasa.consumer.filter

import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.neq
import kolbasa.queue.meta.MetaField
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf

class ConditionTest {

    private val strField = MetaField.ofString("str_value")

    @Test
    fun testAnd() {
        val andCondition = (strField eq "123") and (strField neq "2234")
        assertInstanceOf<AndCondition>(andCondition)
    }

    @Test
    fun testOr() {
        val orCondition = (strField eq "123") or (strField neq "2234")
        assertInstanceOf<OrCondition>(orCondition)
    }

    @Test
    fun testNot() {
        val notCondition1 = (strField eq "123").not()
        assertInstanceOf<NotCondition>(notCondition1)

        // Test that "not" is an operator fun too
        val notCondition2 = !(strField neq "123")
        assertInstanceOf<NotCondition>(notCondition2)
    }

}
