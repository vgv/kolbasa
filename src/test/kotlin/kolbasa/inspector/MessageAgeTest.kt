package kolbasa.inspector

import kolbasa.inspector.MessageAge.Companion.merge
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import java.time.Duration

class MessageAgeTest {

    @Test
    fun testMerge_EmptyList() {
        val result = emptyList<MessageAge>().merge()

        assertNull(result.oldest)
        assertNull(result.newest)
        assertNull(result.oldestReady)
    }

    @Test
    fun testMerge_AllNulls() {
        val ages = listOf(
            MessageAge(oldest = null, newest = null, oldestReady = null),
            MessageAge(oldest = null, newest = null, oldestReady = null),
            MessageAge(oldest = null, newest = null, oldestReady = null)
        )

        val result = ages.merge()

        assertNull(result.oldest)
        assertNull(result.newest)
        assertNull(result.oldestReady)
    }

    @Test
    fun testMerge_SingleElement() {
        val age = MessageAge(
            oldest = Duration.ofMinutes(10),
            newest = Duration.ofSeconds(5),
            oldestReady = Duration.ofMinutes(3)
        )

        val result = listOf(age).merge()

        assertEquals(Duration.ofMinutes(10), result.oldest)
        assertEquals(Duration.ofSeconds(5), result.newest)
        assertEquals(Duration.ofMinutes(3), result.oldestReady)
    }

    @Test
    fun testMerge_OldestPicksMax() {
        val ages = listOf(
            MessageAge(oldest = Duration.ofMinutes(5), newest = null, oldestReady = null),
            MessageAge(oldest = Duration.ofMinutes(10), newest = null, oldestReady = null),
            MessageAge(oldest = Duration.ofMinutes(1), newest = null, oldestReady = null)
        )

        val result = ages.merge()

        assertEquals(Duration.ofMinutes(10), result.oldest)
    }

    @Test
    fun testMerge_NewestPicksMin() {
        val ages = listOf(
            MessageAge(oldest = null, newest = Duration.ofMinutes(5), oldestReady = null),
            MessageAge(oldest = null, newest = Duration.ofSeconds(30), oldestReady = null),
            MessageAge(oldest = null, newest = Duration.ofMinutes(10), oldestReady = null)
        )

        val result = ages.merge()

        assertEquals(Duration.ofSeconds(30), result.newest)
    }

    @Test
    fun testMerge_OldestReadyPicksMax() {
        val ages = listOf(
            MessageAge(oldest = null, newest = null, oldestReady = Duration.ofMinutes(2)),
            MessageAge(oldest = null, newest = null, oldestReady = Duration.ofMinutes(8)),
            MessageAge(oldest = null, newest = null, oldestReady = Duration.ofMinutes(4))
        )

        val result = ages.merge()

        assertEquals(Duration.ofMinutes(8), result.oldestReady)
    }

    @Test
    fun testMerge_NullsAreSkipped() {
        val ages = listOf(
            MessageAge(oldest = null, newest = null, oldestReady = null),
            MessageAge(oldest = Duration.ofMinutes(5), newest = Duration.ofSeconds(10), oldestReady = Duration.ofMinutes(3)),
            MessageAge(oldest = null, newest = null, oldestReady = null)
        )

        val result = ages.merge()

        assertEquals(Duration.ofMinutes(5), result.oldest)
        assertEquals(Duration.ofSeconds(10), result.newest)
        assertEquals(Duration.ofMinutes(3), result.oldestReady)
    }

    @Test
    fun testMerge_MixedNulls() {
        val ages = listOf(
            MessageAge(oldest = Duration.ofMinutes(10), newest = null, oldestReady = Duration.ofMinutes(1)),
            MessageAge(oldest = null, newest = Duration.ofSeconds(5), oldestReady = Duration.ofMinutes(7)),
            MessageAge(oldest = Duration.ofMinutes(3), newest = Duration.ofSeconds(2), oldestReady = null)
        )

        val result = ages.merge()

        assertEquals(Duration.ofMinutes(10), result.oldest)
        assertEquals(Duration.ofSeconds(2), result.newest)
        assertEquals(Duration.ofMinutes(7), result.oldestReady)
    }
}
