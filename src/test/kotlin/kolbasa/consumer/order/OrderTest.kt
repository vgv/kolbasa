package kolbasa.consumer.order

import kolbasa.consumer.order.Order.Companion.asc
import kolbasa.consumer.order.Order.Companion.ascNullsFirst
import kolbasa.consumer.order.Order.Companion.ascNullsLast
import kolbasa.consumer.order.Order.Companion.desc
import kolbasa.consumer.order.Order.Companion.descNullsFirst
import kolbasa.consumer.order.Order.Companion.descNullsLast
import kolbasa.consumer.order.Order.Companion.then
import kolbasa.queue.meta.MetaField
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class OrderTest {

    private val STRING_FIELD = MetaField.string("string_value")
    private val INT_FIELD = MetaField.int("int_value")

    @Test
    fun testThenInfixMethod() {
        val first = STRING_FIELD.ascNullsFirst()
        val second = INT_FIELD.desc()

        val order = first then second

        assertEquals(2, order.size)
        assertSame(first[0], order[0])
        assertSame(second[0], order[1])
    }

    @Test
    fun testOrderFactoryMethods() {
        // OF
        val order = Order.of(STRING_FIELD, SortOrder.DESC)
        assertSame(STRING_FIELD, order.field)
        assertSame(SortOrder.DESC, order.order)

        // ASC
        checkOrder(STRING_FIELD.asc(), SortOrder.ASC)

        // DESC
        checkOrder(STRING_FIELD.desc(), SortOrder.DESC)

        // ASC_NULLS_FIRST
        checkOrder(STRING_FIELD.ascNullsFirst(), SortOrder.ASC_NULLS_FIRST)

        // DESC_NULLS_FIRST
        checkOrder(STRING_FIELD.descNullsFirst(), SortOrder.DESC_NULLS_FIRST)

        // ASC_NULLS_LAST
        checkOrder(STRING_FIELD.ascNullsLast(), SortOrder.ASC_NULLS_LAST)

        // DESC_NULLS_LAST
        checkOrder(STRING_FIELD.descNullsLast(), SortOrder.DESC_NULLS_LAST)
    }

    private fun checkOrder(order: List<Order>, sortOrder: SortOrder) {
        // Every list produced by factory methods (asc, desc etc.) should contain exactly one element
        assertEquals(1, order.size)

        // Let's make sure that sole element has correct values
        val base = order.first()
        assertSame(STRING_FIELD, base.field)
        assertSame(sortOrder, base.order)
        assertEquals("${STRING_FIELD.dbColumnName} ${sortOrder.sql}", base.dbOrderClause)
    }

}
