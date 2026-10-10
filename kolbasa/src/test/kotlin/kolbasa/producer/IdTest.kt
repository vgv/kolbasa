package kolbasa.producer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IdTest {

    @Test
    fun testToStringFormat() {
        // `localId/shard` is a documented, parseable format, so the shard must be a bare number here –
        // ShardId.toString() deliberately isn't one. A round-trip test alone would not notice the difference.
        val expected = "123/45"
        val actual = Id(123, ShardId.of(45)).toString()

        assertEquals(expected, actual)
    }

    @Test
    fun testIdToStringAndBack() {
        // Corner cases
        Id(1, ShardId.MIN_SHARD_ID).also { originalId ->
            val stringId = originalId.toString()
            val parsedId = Id.fromString(stringId)
            assertEquals(originalId, parsedId)
        }
        Id(1, ShardId.of(1)).also { originalId ->
            val stringId = originalId.toString()
            val parsedId = Id.fromString(stringId)
            assertEquals(originalId, parsedId)
        }
        Id(Long.MAX_VALUE, ShardId.MAX_SHARD_ID).also { originalId ->
            val stringId = originalId.toString()
            val parsedId = Id.fromString(stringId)
            assertEquals(originalId, parsedId)
        }

        // Random cases
        (1..1000).forEach { _ ->
            val localId = (1..Long.MAX_VALUE).random()
            val shard = ShardId.random()

            Id(localId, shard).also { originalId ->
                val stringId = originalId.toString()
                val parsedId = Id.fromString(stringId)
                assertEquals(originalId, parsedId)
            }
        }
    }
}
