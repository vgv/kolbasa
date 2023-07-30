package kolbasa.consumer

import kolbasa.schema.Const
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SweepHelperTest {

    @Test
    fun testCheckPeriod_IfAlwaysOn() {
        val iterations = 1_000_000

        // Always false, if probability=0
        (1..iterations).forEach { _ ->
            assertTrue(SweepHelper.checkPeriod(Const.EVERYTIME_SWEEP_PERIOD))
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [3, 5, 13, 25, 143, 567, 34523])
    fun testCheckPeriod(period: Int) {
        // Restart iterations, find next period
        while (!SweepHelper.checkPeriod(period)) {
        }

        // Test
        (1..10).forEach { _ ->
            var iterations = 0
            while (!SweepHelper.checkPeriod(period)) iterations++

            // Test that "false"
            assertEquals(period, iterations + 1)
        }
    }

}
