package kolbasa.consumer.order

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class OrderTest {

    @Test
    fun testFactoryMethods() {
        Order.asc(TestMeta::stringValue).let {
            assertEquals("stringValue", it.metaPropertyName)
            assertEquals(SortOrder.ASC, it.order)
        }
        Order.desc(TestMeta::stringValue).let {
            assertEquals("stringValue", it.metaPropertyName)
            assertEquals(SortOrder.DESC, it.order)
        }
        Order.ascNullsFirst(TestMeta::stringValue).let {
            assertEquals("stringValue", it.metaPropertyName)
            assertEquals(SortOrder.ASC_NULLS_FIRST, it.order)
        }
        Order.ascNullsLast(TestMeta::stringValue).let {
            assertEquals("stringValue", it.metaPropertyName)
            assertEquals(SortOrder.ASC_NULLS_LAST, it.order)
        }
        Order.descNullsFirst(TestMeta::stringValue).let {
            assertEquals("stringValue", it.metaPropertyName)
            assertEquals(SortOrder.DESC_NULLS_FIRST, it.order)
        }
        Order.descNullsLast(TestMeta::stringValue).let {
            assertEquals("stringValue", it.metaPropertyName)
            assertEquals(SortOrder.DESC_NULLS_LAST, it.order)
        }
    }

}

private data class TestMeta(val stringValue: String)
