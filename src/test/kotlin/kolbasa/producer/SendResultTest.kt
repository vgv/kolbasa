package kolbasa.producer

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SendResultTest {

    @Test
    fun test_OnlyFunctions() {
        val first = SendMessage("1")
        val second1 = SendMessage("2-1")
        val second2 = SendMessage("2-2")
        val third = SendMessage("3")
        val fourth = SendMessage("4")

        val firstResult = MessageResult.Success(Id(1, 1), first)
        val secondResult = MessageResult.Error(Exception(), listOf(second1, second2))
        val thirdResult = MessageResult.Duplicate(third)
        val fourthResult = MessageResult.Success(Id(3, 1), fourth)

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
        val first = SendMessage("1")
        val second1 = SendMessage("2-1")
        val second2 = SendMessage("2-2")
        val third = SendMessage("3")

        val firstResult = MessageResult.Success(Id(1, 1), first)
        val secondResult = MessageResult.Error(Exception(), listOf(second1, second2))
        val thirdResult = MessageResult.Success(Id(3, 1), third)

        val sendResult = SendResult(2, listOf(firstResult, secondResult, thirdResult))
        val failedMessages = sendResult.gatherFailedMessages()
        assertEquals(listOf(second1, second2), failedMessages)
    }
}
