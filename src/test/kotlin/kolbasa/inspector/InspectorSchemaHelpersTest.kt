package kolbasa.inspector

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class InspectorSchemaHelpersTest {

    @Test
    fun testCalculateSamplePercent_EmptyTable() {
        // Empty table -> read everything, sample percent is irrelevant, but should be set to READ_ALL_SAMPLE_PERCENT (100%)
        assertEquals(InspectorSchemaHelpers.READ_ALL_SAMPLE_PERCENT, InspectorSchemaHelpers.calculateSamplePercent(0))
    }

    @Test
    fun testCalculateSamplePercent_VerySmallTable_Table_Pages_Less_Than_MIN_PAGES_SCAN() {
        // Very small table, even table pages is less than MIN_PAGES_SCAN
        // In this case we will read all pages, so sample percent should be set to READ_ALL_SAMPLE_PERCENT (100%)
        val pages = InspectorSchemaHelpers.MIN_PAGES_SCAN - 1
        assertEquals(InspectorSchemaHelpers.READ_ALL_SAMPLE_PERCENT, InspectorSchemaHelpers.calculateSamplePercent(pages))
    }

    @Test
    fun testCalculateSamplePercent_SmallTable_Table_Pages_A_Bit_More_Than_MIN_PAGES_SCAN() {
        // Small table -> pages a bit more than MIN_PAGES_SCAN, so we will read MIN_PAGES_SCAN pages, sample percent should be
        // bigger than default (1.0%), but less than 100% and should be calculated as 100.0f * MIN_PAGES_SCAN / pages
        val pages = InspectorSchemaHelpers.MIN_PAGES_SCAN * 2
        val expected = 100.0f * InspectorSchemaHelpers.MIN_PAGES_SCAN / pages
        assertEquals(expected, InspectorSchemaHelpers.calculateSamplePercent(pages))
    }

    @Test
    fun testCalculateSamplePercent_MediumTable_DEFAULT_SAMPLE() {
        // Medium table -> no bounding, relationPages sits between (MIN/DEFAULT_SAMPLE_PERCENT and MAX/DEFAULT_SAMPLE_PERCENT)
        // Sample ration should be exactly DEFAULT_SAMPLE_PERCENT (1%)
        val relationPages =
            ((50 / InspectorSchemaHelpers.DEFAULT_SAMPLE_PERCENT) * (InspectorSchemaHelpers.MIN_PAGES_SCAN + InspectorSchemaHelpers.MAX_PAGES_SCAN)).toLong()

        assertEquals(InspectorSchemaHelpers.DEFAULT_SAMPLE_PERCENT, InspectorSchemaHelpers.calculateSamplePercent(relationPages))
    }

    @Test
    fun testCalculateSamplePercent_LargeTable_BoundedByMax() {
        // Large table -> bounded by MAX_PAGES_SCAN from above, result stays above 0%
        val relationPages = 100_000_000L
        val expected = 100.0f * InspectorSchemaHelpers.MAX_PAGES_SCAN / relationPages
        assertEquals(expected, InspectorSchemaHelpers.calculateSamplePercent(relationPages))
    }

}
