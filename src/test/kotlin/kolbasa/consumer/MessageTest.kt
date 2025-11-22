package kolbasa.consumer

import kolbasa.producer.Id
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class MessageTest {

    @Test
    fun testIsLastAttempt_False() {
        val lastMessage = Message(
            id = Id(1, 1),
            createdAt = 1L,
            processingAt = 1L,
            scheduledAt = 1L,
            remainingAttempts = 0,
            data = "hohoho"
        )
        assertTrue(lastMessage.isLastAttempt(), "Message: $lastMessage")

        val notLastMessage = lastMessage.copy(remainingAttempts = 1)
        assertFalse(notLastMessage.isLastAttempt(), "Message: $notLastMessage")
    }

}
