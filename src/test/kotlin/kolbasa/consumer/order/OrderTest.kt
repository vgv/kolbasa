package kolbasa.consumer.order

import kolbasa.consumer.JavaField
import kolbasa.queue.meta.MetaHelpers
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class OrderTest {

    private val property = TestMeta::stringValue
    private val propertyName = property.name
    private val javaField = JavaField.of(propertyName, property)
    private val metaColumnName = MetaHelpers.generateMetaColumnName(propertyName)

    @Test
    fun testOrderFactoryMethods() {
        // ASC
        checkOrder(Order.asc(property), SortOrder.ASC)
        checkOrder(Order.asc(javaField), SortOrder.ASC)

        // DESC
        checkOrder(Order.desc(property), SortOrder.DESC)
        checkOrder(Order.desc(javaField), SortOrder.DESC)

        // ASC_NULLS_FIRST
        checkOrder(Order.ascNullsFirst(property), SortOrder.ASC_NULLS_FIRST)
        checkOrder(Order.ascNullsFirst(javaField), SortOrder.ASC_NULLS_FIRST)

        // DESC_NULLS_FIRST
        checkOrder(Order.descNullsFirst(property), SortOrder.DESC_NULLS_FIRST)
        checkOrder(Order.descNullsFirst(javaField), SortOrder.DESC_NULLS_FIRST)

        // ASC_NULLS_LAST
        checkOrder(Order.ascNullsLast(property), SortOrder.ASC_NULLS_LAST)
        checkOrder(Order.ascNullsLast(javaField), SortOrder.ASC_NULLS_LAST)

        // DESC_NULLS_LAST
        checkOrder(Order.descNullsLast(property), SortOrder.DESC_NULLS_LAST)
        checkOrder(Order.descNullsLast(javaField), SortOrder.DESC_NULLS_LAST)
    }

    private fun checkOrder(order: Order<TestMeta>, sortOrder: SortOrder) {
        assertEquals(propertyName, order.metaPropertyName)
        assertEquals(sortOrder, order.order)
        assertEquals("$metaColumnName ${sortOrder.sql}", order.dbOrderClause)
    }

}

private data class TestMeta(val stringValue: String)
