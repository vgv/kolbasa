package kolbasa.producer

import kolbasa.schema.Const
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

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

    @Test
    fun testExtractSingularId_CheckIfSendResultIsNotSingular() {
        // Check with zero messages
        val sendResultWithZeroMessages = SendResult<String, Nothing>(
            failedMessages = 0,
            messages = emptyList()
        )
        assertFailsWith<IllegalStateException> { sendResultWithZeroMessages.extractSingularId() }

        // Check with two messages
        val first = SendMessage("1", null, null)
        val second = SendMessage("2", null, null)

        val firstResult = MessageResult.Success(1L, first)
        val secondResult = MessageResult.Success(2L, second)

        val sendResultWithTwoMessages = SendResult(
            failedMessages = 0,
            messages = listOf(firstResult, secondResult)
        )
        assertFailsWith<IllegalStateException> { sendResultWithTwoMessages.extractSingularId() }
    }

    @Test
    fun testExtractSingularId_Success() {
        // Check with two messages
        val first = SendMessage("1", null, null)

        val firstResult = MessageResult.Success(1L, first)

        val sendResult = SendResult(
            failedMessages = 0,
            messages = listOf(firstResult)
        )

        assertEquals(1, sendResult.extractSingularId())
    }

    @Test
    fun testExtractSingularId_Duplicate() {
        // Check with two messages
        val first = SendMessage("1", null, null)

        val firstResult = MessageResult.Duplicate(first)

        val sendResult = SendResult(
            failedMessages = 0,
            messages = listOf(firstResult)
        )

        assertEquals(Const.RESERVED_DUPLICATE_ID, sendResult.extractSingularId())
    }

    @Test
    fun testExtractSingularId_Error() {
        // Check with two messages
        val first = SendMessage("1", null, null)
        val exception = RuntimeException()

        val firstResult = MessageResult.Error(exception, listOf(first))

        val sendResult = SendResult(
            failedMessages = 1,
            messages = listOf(firstResult)
        )

        assertFailsWith<RuntimeException> { sendResult.extractSingularId() }
    }
}
