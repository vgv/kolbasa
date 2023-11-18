package kolbasa.producer

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SendResultTest {

    @Test
    fun test_OnlyFunctions() {
        val first = SendMessage("1", null, null)
        val second1 = SendMessage("2-1", null, null)
        val second2 = SendMessage("2-2", null, null)
        val third = SendMessage("3", null, null)
        val fourth = SendMessage("4", null, null)

        val firstResult = MessageResult.Success(1L, first)
        val secondResult = MessageResult.Error(Exception(), listOf(second1, second2))
        val thirdResult = MessageResult.Duplicate(third)
        val fourthResult = MessageResult.Success(3L, fourth)

        val sendResult = SendResult(
            failedMessages = 2,
            messages = listOf(firstResult, secondResult, thirdResult, fourthResult)
        )

        assertEquals(listOf(firstResult, fourthResult), sendResult.onlySuccessful())
        assertEquals(listOf(secondResult), sendResult.onlyFailed())
        assertEquals(listOf(thirdResult), sendResult.onlyDuplicated())
    }

    @Test
    fun testGatherFailedMessages() {
        val first = SendMessage("1", null, null)
        val second1 = SendMessage("2-1", null, null)
        val second2 = SendMessage("2-2", null, null)
        val third = SendMessage("3", null, null)

        val firstResult = MessageResult.Success(1L, first)
        val secondResult = MessageResult.Error(Exception(), listOf(second1, second2))
        val thirdResult = MessageResult.Success(3L, third)

        val sendResult = SendResult(2, listOf(firstResult, secondResult, thirdResult))
        val failedMessages = sendResult.gatherFailedMessages()
        assertEquals(listOf(second1, second2), failedMessages)
    }

}
