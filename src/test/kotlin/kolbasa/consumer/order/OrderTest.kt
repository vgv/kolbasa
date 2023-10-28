package kolbasa.consumer.order

import kolbasa.consumer.JavaField
import kolbasa.consumer.order.Order.Companion.asc
import kolbasa.consumer.order.Order.Companion.ascNullsFirst
import kolbasa.consumer.order.Order.Companion.ascNullsLast
import kolbasa.consumer.order.Order.Companion.desc
import kolbasa.consumer.order.Order.Companion.descNullsFirst
import kolbasa.consumer.order.Order.Companion.descNullsLast
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
        checkOrder(property.asc(), SortOrder.ASC)
        checkOrder(javaField.asc(), SortOrder.ASC)

        // DESC
        checkOrder(property.desc(), SortOrder.DESC)
        checkOrder(javaField.desc(), SortOrder.DESC)

        // ASC_NULLS_FIRST
        checkOrder(property.ascNullsFirst(), SortOrder.ASC_NULLS_FIRST)
        checkOrder(javaField.ascNullsFirst(), SortOrder.ASC_NULLS_FIRST)

        // DESC_NULLS_FIRST
        checkOrder(property.descNullsFirst(), SortOrder.DESC_NULLS_FIRST)
        checkOrder(javaField.descNullsFirst(), SortOrder.DESC_NULLS_FIRST)

        // ASC_NULLS_LAST
        checkOrder(property.ascNullsLast(), SortOrder.ASC_NULLS_LAST)
        checkOrder(javaField.ascNullsLast(), SortOrder.ASC_NULLS_LAST)

        // DESC_NULLS_LAST
        checkOrder(property.descNullsLast(), SortOrder.DESC_NULLS_LAST)
        checkOrder(javaField.descNullsLast(), SortOrder.DESC_NULLS_LAST)
    }

    private fun checkOrder(order: Order<TestMeta>, sortOrder: SortOrder) {
        assertEquals(propertyName, order.metaPropertyName)
        assertEquals(metaColumnName, order.dbColumnName)
        assertEquals(sortOrder, order.order)
        assertEquals("$metaColumnName ${sortOrder.sql}", order.dbOrderClause)
    }

}

private data class TestMeta(val stringValue: String)
