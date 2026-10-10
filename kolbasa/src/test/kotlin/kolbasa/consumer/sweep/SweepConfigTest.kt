package kolbasa.consumer.sweep

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SweepConfigTest {

    @Test
    fun testConfigBuilder() {
        val config = SweepConfig
            .builder()
            .probability(0.42)
            .maxMessages(4200)
            .build()

        assertEquals(0.42, config.probability)
        assertEquals(4200, config.maxMessages)
    }

    @Test
    fun testConfigBuilder_Check_Disable() {
        val config = SweepConfig
            .builder()
            .disable()
            .build()

        assertEquals(SweepConfig.SWEEP_IS_DISABLED, config.probability)
    }

}
