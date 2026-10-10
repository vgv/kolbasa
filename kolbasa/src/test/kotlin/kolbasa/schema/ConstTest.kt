package kolbasa.schema

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ConstTest {

    @Test
    fun testQueueCompanionSuffixMaxLength() {
        assertEquals(4, Const.QUEUE_COMPANION_SUFFIX_MAX_LENGTH)
    }

    @Test
    fun testDlqSuffixLengthDoesNotExceedMax() {
        assertTrue(Const.DLQ_TABLE_NAME_SUFFIX.length <= Const.QUEUE_COMPANION_SUFFIX_MAX_LENGTH)
    }

    @Test
    fun testArchiveSuffixLengthDoesNotExceedMax() {
        assertTrue(Const.ARCHIVE_TABLE_NAME_SUFFIX.length <= Const.QUEUE_COMPANION_SUFFIX_MAX_LENGTH)
    }

    @Test
    fun testQueueNameMaxLengthAccountsForSuffix() {
        assertEquals(
            Const.MAX_DATABASE_OBJECT_NAME_LENGTH - Const.QUEUE_TABLE_NAME_PREFIX.length - Const.QUEUE_COMPANION_SUFFIX_MAX_LENGTH,
            Const.QUEUE_NAME_MAX_LENGTH
        )
    }
}
