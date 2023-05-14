package kolbasa.task

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

class StatsConfigTest {

    @Test
    fun testDefaultLockHashes() {
        val config = StatsConfig()

        assertEquals(-1199941354, config.realtimeDumpLockId)
        assertEquals(1751629472, config.allDumpLockId)
        assertEquals(-161380488, config.deleteOutdatedMeasuresLockId)
        assertEquals(1961645797, config.deleteOutdatedQueuesLockId)
    }
}
