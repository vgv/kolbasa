package kolbasa.consumer

import kolbasa.producer.Id
import kolbasa.producer.ShardId
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

class MessageTest {

    @Test
    fun testIsLastAttempt_False() {
        val lastMessage = Message(
            id = Id(1, ShardId.of(1)),
            createdAt = Instant.EPOCH,
            processingAt = Instant.EPOCH,
            scheduledAt = Instant.EPOCH,
            remainingAttempts = 0,
            data = "hohoho"
        )
        assertTrue(lastMessage.isLastAttempt(), "Message: $lastMessage")

        val notLastMessage = lastMessage.copy(remainingAttempts = 1)
        assertFalse(notLastMessage.isLastAttempt(), "Message: $notLastMessage")
    }

}
