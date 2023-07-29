package kolbasa.consumer

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SweepHelperTest {

    @Test
    fun testProbabilityCornerCases() {
        val iterations = 1_000_000

        // Always false, if probability=0
        (1..iterations).forEach { _ ->
            assertFalse(SweepHelper.probability(0))
        }

        // Always false, if probability=100
        (1..iterations).forEach { _ ->
            assertTrue(SweepHelper.probability(100))
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 5, 20, 50, 75, 95, 99])
    fun testProbability(probability: Int) {
        val iterations = 1_000_000

        var yes = 0
        var no = 0
        (1..iterations).forEach { _ ->
            if (SweepHelper.probability(probability)) {
                yes++
            } else {
                no++
            }
        }

        // Just quick check
        assertEquals(iterations, yes + no)

        val onePercent = iterations / 100

        // Test positive
        val yesIterations = iterations * probability / 100
        assertTrue(yes in (yesIterations - onePercent)..(yesIterations + onePercent), "yes: $yes, yesIterations: $yesIterations, onePercent: $onePercent")

        // Test negative
        val noIterations = iterations * (100 - probability) / 100
        assertTrue(no in (noIterations - onePercent)..(noIterations + onePercent), "no: $no, noIterations: $noIterations, onePercent: $onePercent")
    }


}
