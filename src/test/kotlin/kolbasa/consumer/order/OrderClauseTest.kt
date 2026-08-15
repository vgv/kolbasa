package kolbasa.consumer.order

import kolbasa.queue.meta.MetaField
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class OrderClauseTest {

    private val stringField = MetaField.ofString("string_value")

    @Test
    fun testDbOrderClause() {
        val orderClause = OrderClause(stringField, SortOrder.DESC_NULLS_FIRST)

        val expected = "${stringField.dbColumnName} ${SortOrder.DESC_NULLS_FIRST.sql}"
        assertEquals(expected, orderClause.dbOrderClause)
    }

}
