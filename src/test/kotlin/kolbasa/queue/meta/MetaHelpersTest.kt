package kolbasa.queue.meta

import kolbasa.queue.meta.MetaHelpers.findEnumValueOfFunction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

internal class MetaHelpersTest {

    enum class TestColor {
        RED, GREEN, YELLOW
    }

    @Test
    fun testGenerateMetaColumnName() {
        assertEquals("meta_int_value", MetaHelpers.generateMetaColumnName("intValue"))
        assertEquals("meta_long_value", MetaHelpers.generateMetaColumnName("LongValue"))

        assertEquals("meta_very_very_long_field_name", MetaHelpers.generateMetaColumnName("veryVeryLongFieldName"))
    }

    @Test
    fun testFindEnumValueOfFunction() {
        // Real enum
        assertEquals(TestColor::valueOf, findEnumValueOfFunction(TestColor::class))

        // Any other class
        assertNull(findEnumValueOfFunction(MetaHelpersTest::class))
    }
}
