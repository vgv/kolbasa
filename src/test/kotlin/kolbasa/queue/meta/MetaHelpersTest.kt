package kolbasa.queue.meta

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class MetaHelpersTest {

    @Test
    fun testGenerateMetaColumnName() {
        assertEquals("meta_int_value", MetaHelpers.generateMetaColumnName("intValue"))
        assertEquals("meta_int_value", MetaHelpers.generateMetaColumnName("int_value"))

        assertEquals("meta_long_value", MetaHelpers.generateMetaColumnName("LongValue"))
        assertEquals("meta_long_value", MetaHelpers.generateMetaColumnName("Long_Value"))

        assertEquals("meta_very_very_long_field_name", MetaHelpers.generateMetaColumnName("veryVeryLongFieldName"))
        assertEquals("meta_very_very_long_field_name", MetaHelpers.generateMetaColumnName("very_Very_Long_Field_Name"))
    }
}
