package kolbasa.producer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class IdTest {

    @Test
    fun testIdToStringAndBack() {
        // Corner cases
        Id(1, 1).also {
            val stringId = it.toString()
            val parsedId = Id.fromString(stringId)
            assertEquals(it, parsedId)
        }
        Id(Long.MAX_VALUE, Int.MAX_VALUE).also {
            val stringId = it.toString()
            val parsedId = Id.fromString(stringId)
            assertEquals(it, parsedId)
        }

        // Random cases
        (1..1000).forEach { _ ->
            val localId = (1..Long.MAX_VALUE).random()
            val shard = (1..Int.MAX_VALUE).random()

            Id(localId, shard).also {
                val stringId = it.toString()
                val parsedId = Id.fromString(stringId)
                assertEquals(it, parsedId)
            }
        }
    }


}
