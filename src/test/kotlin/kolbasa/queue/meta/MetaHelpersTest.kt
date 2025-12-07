package kolbasa.queue.meta

import kolbasa.queue.QueueHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class MetaHelpersTest {

    @Test
    fun testGenerateMetaColumnName() {
        assertEquals("meta_int_value", QueueHelpers.generateMetaColumnDbName("intValue"))
        assertEquals("meta_int_value", QueueHelpers.generateMetaColumnDbName("int_value"))

        assertEquals("meta_long_value", QueueHelpers.generateMetaColumnDbName("LongValue"))
        assertEquals("meta_long_value", QueueHelpers.generateMetaColumnDbName("Long_Value"))

        assertEquals("meta_very_very_long_field_name", QueueHelpers.generateMetaColumnDbName("veryVeryLongFieldName"))
        assertEquals("meta_very_very_long_field_name", QueueHelpers.generateMetaColumnDbName("very_Very_Long_Field_Name"))
    }
}
