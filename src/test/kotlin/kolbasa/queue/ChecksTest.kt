package kolbasa.queue

import kolbasa.assertNotNull
import kolbasa.cluster.ClusterStateUpdateConfig
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.inspector.CountOptions
import kolbasa.inspector.DistinctValuesOptions
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.AddScheduledAt
import kolbasa.mutator.SetRemainingAttempts
import kolbasa.mutator.SetScheduledAt
import kolbasa.queue.meta.MetaField
import kolbasa.schema.Const
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import kotlin.random.Random

internal class ChecksTest {

    @Test
    fun testCheckDelay_Null_Works() {
        // check NULL doesn't fail
        Checks.checkDelay(null)
    }

    @Test
    fun testCheckDelay_ZeroOrPositive_Works() {
        //Check all values >= zero work
        Checks.checkDelay(Duration.ofMillis(0))
        Checks.checkDelay(Duration.ofMillis(Random.nextLong(1, Long.MAX_VALUE)))
    }

    @Test
    fun testCheckDelay_Negative_Fails() {
        // Check other negative values fail
        assertThrows<IllegalArgumentException> {
            Checks.checkDelay(Duration.ofMillis(Random.nextLong(Long.MIN_VALUE, 0)))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckAttempts_Null_Works() {
        // check NULL works
        Checks.checkAttempts(null)
    }

    @Test
    fun testCheckAttempts_Positive_Works() {
        //Check all positive values work
        Checks.checkAttempts(Random.nextInt(1, Int.MAX_VALUE))
    }

    @Test
    fun testCheckAttempts_ZeroOrNegative_Fails() {
        // Check other negative or zero values fail
        assertThrows<IllegalArgumentException> {
            Checks.checkAttempts(0)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkAttempts(Random.nextInt(Int.MIN_VALUE, 0))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckProducerName() {
        Checks.checkProducerName(null)
        Checks.checkProducerName("just value shorter than 255 symbols")

        // too long name
        assertThrows<IllegalArgumentException> {
            val longName = "a".repeat(Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH + 1)
            Checks.checkProducerName(longName)
        }

        // wrong symbols
        assertThrows<IllegalArgumentException> {
            val wrongName = "producer;name"
            Checks.checkProducerName(wrongName)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testChunkSize_ZeroOrNegative_Fails() {
        assertThrows<IllegalArgumentException> {
            Checks.checkChunkSize(0)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkChunkSize(-1)
        }

        // any value >= 1 should pass
        assertDoesNotThrow {
            Checks.checkChunkSize(Random.nextInt(1, 1_000_000))
        }

        // null should pass
        assertDoesNotThrow {
            Checks.checkChunkSize(null)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckConsumerName() {
        Checks.checkConsumerName(null)
        Checks.checkConsumerName("just value shorter than 255 symbols")

        // too long name
        assertThrows<IllegalArgumentException> {
            val longName = "a".repeat(Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH + 1)
            Checks.checkConsumerName(longName)
        }

        // wrong symbols
        assertThrows<IllegalArgumentException> {
            val wrongName = "consumer;name"
            Checks.checkConsumerName(wrongName)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckVisibilityTimeout_VisibilityTimeout_Null_Works() {
        // check NULL works
        Checks.checkVisibilityTimeout(null)
    }

    @Test
    fun testCheckVisibilityTimeout_ZeroOrPositive_Works() {
        //Check all values >= zero work
        Checks.checkVisibilityTimeout(Duration.ofMillis(0))
        Checks.checkVisibilityTimeout(Duration.ofMillis(Random.nextLong(1, Long.MAX_VALUE)))
    }

    @Test
    fun testCheckVisibilityTimeout_Negative_Fails() {
        // Check other negative values fail
        assertThrows<IllegalArgumentException> {
            Checks.checkVisibilityTimeout(Duration.ofMillis(Random.nextLong(Long.MIN_VALUE, 0)))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckQueueName_IfEmpty() {
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("", QueueRole.MAIN)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("", QueueRole.DLQ)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("", QueueRole.ARCHIVE)
        }
    }

    @Test
    fun testCheckQueueName_InvalidPrefix() {
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("q_customer_email", QueueRole.MAIN)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("q_customer_email_dlq", QueueRole.DLQ)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("q_customer_email_arc", QueueRole.ARCHIVE)
        }
    }

    @Test
    fun testCheckQueueName_TooLong() {
        val longName = "a".repeat(Const.QUEUE_NAME_MAX_LENGTH + 1)
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName(longName, QueueRole.MAIN)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName(longName, QueueRole.DLQ)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName(longName, QueueRole.ARCHIVE)
        }
    }

    @Test
    fun testCheckQueueName_InvalidSymbols() {
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("queue$", QueueRole.MAIN)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("queue\$_dlq", QueueRole.DLQ)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("queue\$_arc", QueueRole.ARCHIVE)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckQueueName_MainQueueCannotEndWithDlqSuffix() {
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("orders_dlq", QueueRole.MAIN)
        }
    }

    @Test
    fun testCheckQueueName_MainQueueCannotEndWithArchiveSuffix() {
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("orders_arc", QueueRole.MAIN)
        }
    }

    @Test
    fun testCheckQueueName_DlqQueueMustEndWithDlqSuffix() {
        assertDoesNotThrow {
            Checks.checkQueueName("orders_dlq", QueueRole.DLQ)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("orders", QueueRole.DLQ)
        }
    }

    @Test
    fun testCheckQueueName_ArchiveQueueMustEndWithArchiveSuffix() {
        assertDoesNotThrow {
            Checks.checkQueueName("orders_arc", QueueRole.ARCHIVE)
        }
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueName("orders", QueueRole.ARCHIVE)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckQueueRole_MainCanHaveDlq() {
        assertDoesNotThrow {
            Checks.checkQueueRole(QueueRole.MAIN, QueueOptions(dlqOptions = DlqOptions.DEFAULT))
        }
    }

    @Test
    fun testCheckQueueRole_MainCanHaveArchive() {
        assertDoesNotThrow {
            Checks.checkQueueRole(QueueRole.MAIN, QueueOptions(archiveQueueOptions = ArchiveQueueOptions.DEFAULT))
        }
    }

    @Test
    fun testCheckQueueRole_DlqCannotHaveDlq() {
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueRole(QueueRole.DLQ, QueueOptions(dlqOptions = DlqOptions.DEFAULT))
        }
    }

    @Test
    fun testCheckQueueRole_ArchiveCannotHaveArchive() {
        assertThrows<IllegalArgumentException> {
            Checks.checkQueueRole(QueueRole.ARCHIVE, QueueOptions(archiveQueueOptions = ArchiveQueueOptions.DEFAULT))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckDlqRetention_TooShort() {
        assertThrows<IllegalArgumentException> {
            Checks.checkDlqRetention(DlqOptions.MIN_RETENTION.minusNanos(1))
        }
    }

    @Test
    fun testCheckDlqRetention_TooLong() {
        assertThrows<IllegalArgumentException> {
            Checks.checkDlqRetention(DlqOptions.MAX_RETENTION.plusNanos(1))
        }
    }

    @Test
    fun testCheckDlqRetention_Valid() {
        assertDoesNotThrow { Checks.checkDlqRetention(DlqOptions.MIN_RETENTION) }
        assertDoesNotThrow { Checks.checkDlqRetention(DlqOptions.MAX_RETENTION) }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckArchiveQueueRetention_TooShort() {
        assertThrows<IllegalArgumentException> {
            Checks.checkArchiveQueueRetention(ArchiveQueueOptions.MIN_RETENTION.minusNanos(1))
        }
    }

    @Test
    fun testCheckArchiveQueueRetention_TooLong() {
        assertThrows<IllegalArgumentException> {
            Checks.checkArchiveQueueRetention(ArchiveQueueOptions.MAX_RETENTION.plusNanos(1))
        }
    }

    @Test
    fun testCheckArchiveQueueRetention_Valid() {
        assertDoesNotThrow { Checks.checkArchiveQueueRetention(ArchiveQueueOptions.MIN_RETENTION) }
        assertDoesNotThrow { Checks.checkArchiveQueueRetention(ArchiveQueueOptions.MAX_RETENTION) }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckRetentionMaxMessages_Positive() {
        assertDoesNotThrow { Checks.checkRetentionMaxMessages(1) }
        assertDoesNotThrow { Checks.checkRetentionMaxMessages(null) }
    }

    @Test
    fun testCheckRetentionMaxMessages_ZeroOrNegative() {
        assertThrows<IllegalArgumentException> { Checks.checkRetentionMaxMessages(0) }
        assertThrows<IllegalArgumentException> { Checks.checkRetentionMaxMessages(-1) }
    }

    // ---------------------------------------------------------------------------------------------------------------
    @Test
    fun testCheckMetaFieldsUnique() {
        val fields = listOf(
            MetaField.ofInt("user_id"),
            MetaField.ofLong("userId"),
            MetaField.ofString("USER_ID"),
        )

        val exception = assertThrows<IllegalArgumentException> { Checks.checkMetaFieldsUnique(fields) }
        val message = assertNotNull(exception.message)
        assertEquals("Meta fields [user_id, userId, USER_ID] all map to the same database column 'meta_user_id'", message)
    }
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckUserDefinedMetaFieldName_CannotEndWithReservedSuffix() {
        // DLQ
        assertThrows<IllegalArgumentException> { Checks.checkUserDefinedMetaFieldName("field_dlq") }
        assertThrows<IllegalArgumentException> { Checks.checkUserDefinedMetaFieldName("FIELD_DLQ") }
        assertThrows<IllegalArgumentException> { Checks.checkUserDefinedMetaFieldName("fieldDlq") }

        // Archive queues
        assertThrows<IllegalArgumentException> { Checks.checkUserDefinedMetaFieldName("field_arc") }
        assertThrows<IllegalArgumentException> { Checks.checkUserDefinedMetaFieldName("fieldArc") }
        assertThrows<IllegalArgumentException> { Checks.checkUserDefinedMetaFieldName("FIELD_ARC") }
    }

    @Test
    fun testCheckUserDefinedMetaFieldName_ValidNames() {
        assertDoesNotThrow { Checks.checkUserDefinedMetaFieldName("user_id") }
        assertDoesNotThrow { Checks.checkUserDefinedMetaFieldName("field") }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckMetaFieldName_IfEmpty() {
        assertThrows<IllegalArgumentException> {
            Checks.checkMetaFieldName("")
        }
    }

    @Test
    fun testCheckMetaFieldName_TooLong() {
        val longName = "a".repeat(Const.META_FIELD_NAME_MAX_LENGTH + 1)
        assertThrows<IllegalArgumentException> {
            Checks.checkMetaFieldName(longName)
        }
    }

    @Test
    fun testCheckMetaFieldName_InvalidSymbols() {
        assertThrows<IllegalArgumentException> {
            Checks.checkMetaFieldName("meta$")
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckSweepMaxMessages_LessThanMin() {
        assertThrows<IllegalArgumentException> {
            Checks.checkSweepMaxMessages(SweepConfig.MIN_SWEEP_MESSAGES - 1)
        }
    }

    @Test
    fun testCheckSweepMaxMessages_MoreThanMax() {
        assertThrows<IllegalArgumentException> {
            Checks.checkSweepMaxMessages(SweepConfig.MAX_SWEEP_MESSAGES + 1)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckSweepProbability_LessThanMin() {
        assertThrows<IllegalArgumentException> {
            Checks.checkSweepProbability(SweepConfig.MIN_SWEEP_PROBABILITY - Math.ulp(SweepConfig.MIN_SWEEP_PROBABILITY))
        }
    }

    @Test
    fun testCheckSweepPeriod_MoreThanMax() {
        assertThrows<IllegalArgumentException> {
            Checks.checkSweepProbability(SweepConfig.MAX_SWEEP_PROBABILITY + Math.ulp(SweepConfig.MAX_SWEEP_PROBABILITY))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckClusterStateUpdateInterval() {
        assertThrows<IllegalArgumentException> {
            val ulp = Duration.ofNanos(1)
            val aBitSmaller = ClusterStateUpdateConfig.MIN_INTERVAL - ulp
            Checks.checkClusterStateUpdateInterval(aBitSmaller)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckSamplePercent_ValidValues() {
        // special cases
        assertDoesNotThrow { Checks.checkSamplePercent(CountOptions.YOU_KNOW_BETTER) }
        assertDoesNotThrow { Checks.checkSamplePercent(DistinctValuesOptions.YOU_KNOW_BETTER) }

        assertDoesNotThrow { Checks.checkSamplePercent(0.0001f) }
        assertDoesNotThrow { Checks.checkSamplePercent(50.0f) }
        assertDoesNotThrow { Checks.checkSamplePercent(100.0f) }
    }

    @Test
    fun testCheckSamplePercent_InvalidValues() {
        assertThrows<IllegalArgumentException> { Checks.checkSamplePercent(0.0f) }
        assertThrows<IllegalArgumentException> { Checks.checkSamplePercent(-1.0f) }
        assertThrows<IllegalArgumentException> { Checks.checkSamplePercent(100.01f) }
    }

    // ---------------------------------------------------------------------------------------------------------------
    @Test
    fun testCheckMutations_Ok() {
        // One mutation
        Checks.checkMutations(listOf(AddRemainingAttempts(1)))

        // Two mutations
        Checks.checkMutations(listOf(AddRemainingAttempts(1), AddScheduledAt(Duration.ZERO)))
    }

    @Test
    fun testCheckMutations_Error() {
        // Only remaining_attempts field mutations
        assertThrows<IllegalArgumentException> {
            Checks.checkMutations(listOf(AddRemainingAttempts(1), SetRemainingAttempts(2)))
        }

        // Only scheduled_at field mutations
        assertThrows<IllegalArgumentException> {
            Checks.checkMutations(listOf(AddScheduledAt(Duration.ZERO), SetScheduledAt(Duration.ZERO)))
        }

        // More than one field
        assertThrows<IllegalArgumentException> {
            Checks.checkMutations(
                listOf(
                    AddScheduledAt(Duration.ZERO),
                    SetScheduledAt(Duration.ZERO),
                    AddRemainingAttempts(1)
                )
            )
        }
    }
}
