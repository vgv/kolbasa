package kolbasa.producer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

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

    @Test
    fun testGatherExceptions() {
        val first = SendMessage("1")
        val second1 = SendMessage("2-1")
        val second2 = SendMessage("2-2")
        val third = SendMessage("3")

        val firstEx = Exception("First exception")
        val secondEx = Exception("Second exception")
        val thirdEx = Exception("Third exception")

        val firstResult =  MessageResult.Error(firstEx, listOf(first))
        val secondResult = MessageResult.Error(secondEx, listOf(second1, second2))
        val thirdResult =  MessageResult.Error(thirdEx, listOf(third))

        val sendResult = SendResult(4, listOf(firstResult, secondResult, thirdResult))
        val exceptions = sendResult.gatherExceptions()
        assertEquals(listOf(firstEx, secondEx, thirdEx), exceptions)
    }

    @Test
    fun testThrowExceptionIfAny_No_Suppression() {
        val first = SendMessage("1")
        val second1 = SendMessage("2-1")
        val second2 = SendMessage("2-2")
        val third = SendMessage("3")

        val firstEx = Exception("First exception")
        val secondEx = Exception("Second exception")
        val thirdEx = Exception("Third exception")

        val firstResult =  MessageResult.Error(firstEx, listOf(first))
        val secondResult = MessageResult.Error(secondEx, listOf(second1, second2))
        val thirdResult =  MessageResult.Error(thirdEx, listOf(third))

        val sendResult = SendResult(4, listOf(firstResult, secondResult, thirdResult))

        val exception = assertThrows<Throwable> {
            sendResult.throwExceptionIfAny(addOthersAsSuppressed = false)
        }

        assertSame(firstEx, exception)
        assertTrue(exception.suppressed.isEmpty())
    }

    @Test
    fun testThrowExceptionIfAny_With_Suppression() {
        val first = SendMessage("1")
        val second1 = SendMessage("2-1")
        val second2 = SendMessage("2-2")
        val third = SendMessage("3")

        val firstEx = Exception("First exception")
        val secondEx = Exception("Second exception")
        val thirdEx = Exception("Third exception")

        val firstResult =  MessageResult.Error(firstEx, listOf(first))
        val secondResult = MessageResult.Error(secondEx, listOf(second1, second2))
        val thirdResult =  MessageResult.Error(thirdEx, listOf(third))

        val sendResult = SendResult(4, listOf(firstResult, secondResult, thirdResult))

        val exception = assertThrows<Throwable> {
            sendResult.throwExceptionIfAny(addOthersAsSuppressed = true)
        }

        assertSame(firstEx, exception)
        assertSame(secondEx, exception.suppressed[0])
        assertSame(thirdEx, exception.suppressed[1])
    }
}
