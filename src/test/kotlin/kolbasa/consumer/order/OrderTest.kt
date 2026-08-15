package kolbasa.consumer.order

import kolbasa.consumer.order.Order.Companion.asc
import kolbasa.consumer.order.Order.Companion.ascNullsFirst
import kolbasa.consumer.order.Order.Companion.ascNullsLast
import kolbasa.consumer.order.Order.Companion.desc
import kolbasa.consumer.order.Order.Companion.descNullsFirst
import kolbasa.consumer.order.Order.Companion.descNullsLast
import kolbasa.queue.meta.MetaField
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

class OrderTest {

    private val stringField = MetaField.ofString("string_value")
    private val intField = MetaField.ofInt("int_value")

    @Test
    fun testThenInfixMethod() {
        val first = stringField.ascNullsFirst()
        val second = intField.desc()
        val third = stringField.descNullsLast()

        val order = first then second then third

        assertEquals(3, order.clauses.size)
        assertSame(first.clauses[0], order.clauses[0])
        assertSame(second.clauses[0], order.clauses[1])
        assertSame(third.clauses[0], order.clauses[2])
    }

    @Test
    fun testByMethod() {
        val first = stringField.ascNullsFirst()
        val second = intField.desc()
        val third = stringField.descNullsLast()

        val order = Order.by(first, second, third)

        assertEquals(3, order.clauses.size)
        assertSame(first.clauses[0], order.clauses[0])
        assertSame(second.clauses[0], order.clauses[1])
        assertSame(third.clauses[0], order.clauses[2])
    }

    @Test
    fun testOrderFactoryMethods() {
        // OF
        val order = Order.by(stringField, SortOrder.DESC)
        assertEquals(1, order.clauses.size)
        assertSame(stringField, order.clauses[0].field)
        assertSame(SortOrder.DESC, order.clauses[0].order)

        // ASC
        checkOrder(stringField.asc(), SortOrder.ASC)

        // DESC
        checkOrder(stringField.desc(), SortOrder.DESC)

        // ASC_NULLS_FIRST
        checkOrder(stringField.ascNullsFirst(), SortOrder.ASC_NULLS_FIRST)

        // DESC_NULLS_FIRST
        checkOrder(stringField.descNullsFirst(), SortOrder.DESC_NULLS_FIRST)

        // ASC_NULLS_LAST
        checkOrder(stringField.ascNullsLast(), SortOrder.ASC_NULLS_LAST)

        // DESC_NULLS_LAST
        checkOrder(stringField.descNullsLast(), SortOrder.DESC_NULLS_LAST)
    }

    private fun checkOrder(order: Order, sortOrder: SortOrder) {
        // Every list produced by factory methods (asc, desc etc.) should contain exactly one element
        assertEquals(1, order.clauses.size)

        // Let's make sure that sole element has correct values
        val base = order.clauses.first()
        assertSame(stringField, base.field)
        assertSame(sortOrder, base.order)
    }

}
