package kolbasa.queue

import kolbasa.cluster.ClusterStateUpdateConfig
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.inspector.CountOptions
import kolbasa.inspector.DistinctValuesOptions
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.AddScheduledAt
import kolbasa.mutator.SetRemainingAttempts
import kolbasa.mutator.SetScheduledAt
import kolbasa.schema.Const
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
        assertThrows<IllegalStateException> {
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
        assertThrows<IllegalStateException> {
            Checks.checkAttempts(0)
        }
        assertThrows<IllegalStateException> {
            Checks.checkAttempts(Random.nextInt(Int.MIN_VALUE, 0))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckProducerName() {
        Checks.checkProducerName(null)
        Checks.checkProducerName("just value shorter than 255 symbols")

        // too long name
        assertThrows<IllegalStateException> {
            val longName = "a".repeat(Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH + 1)
            Checks.checkProducerName(longName)
        }

        // wrong symbols
        assertThrows<IllegalStateException> {
            val wrongName = "producer;name"
            Checks.checkProducerName(wrongName)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testBatchSize_ZeroOrNegative_Fails() {
        assertThrows<IllegalStateException> {
            Checks.checkBatchSize(0)
        }
        assertThrows<IllegalStateException> {
            Checks.checkBatchSize(-1)
        }

        // any value >= 1 should pass
        assertDoesNotThrow {
            Checks.checkBatchSize(Random.nextInt(1, 1_000_000))
        }

        // null should pass
        assertDoesNotThrow {
            Checks.checkBatchSize(null)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckConsumerName() {
        Checks.checkConsumerName(null)
        Checks.checkConsumerName("just value shorter than 255 symbols")

        // too long name
        assertThrows<IllegalStateException> {
            val longName = "a".repeat(Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH + 1)
            Checks.checkConsumerName(longName)
        }

        // wrong symbols
        assertThrows<IllegalStateException> {
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
        assertThrows<IllegalStateException> {
            Checks.checkVisibilityTimeout(Duration.ofMillis(Random.nextLong(Long.MIN_VALUE, 0)))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckQueueName_IfEmpty() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("", QueueType.MAIN)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("", QueueType.DLQ)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("", QueueType.ARCHIVE)
        }
    }

    @Test
    fun testCheckQueueName_InvalidPrefix() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("q_customer_email", QueueType.MAIN)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("q_customer_email_dlq", QueueType.DLQ)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("q_customer_email_arc", QueueType.ARCHIVE)
        }
    }

    @Test
    fun testCheckQueueName_TooLong() {
        val longName = "a".repeat(Const.QUEUE_NAME_MAX_LENGTH + 1)
        assertThrows<IllegalStateException> {
            Checks.checkQueueName(longName, QueueType.MAIN)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName(longName, QueueType.DLQ)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName(longName, QueueType.ARCHIVE)
        }
    }

    @Test
    fun testCheckQueueName_InvalidSymbols() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("queue$", QueueType.MAIN)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("queue\$_dlq", QueueType.DLQ)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("queue\$_arc", QueueType.ARCHIVE)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckQueueName_MainQueueCannotEndWithDlqSuffix() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("orders_dlq", QueueType.MAIN)
        }
    }

    @Test
    fun testCheckQueueName_MainQueueCannotEndWithArchiveSuffix() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("orders_arc", QueueType.MAIN)
        }
    }

    @Test
    fun testCheckQueueName_DlqQueueMustEndWithDlqSuffix() {
        assertDoesNotThrow {
            Checks.checkQueueName("orders_dlq", QueueType.DLQ)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("orders", QueueType.DLQ)
        }
    }

    @Test
    fun testCheckQueueName_ArchiveQueueMustEndWithArchiveSuffix() {
        assertDoesNotThrow {
            Checks.checkQueueName("orders_arc", QueueType.ARCHIVE)
        }
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("orders", QueueType.ARCHIVE)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckQueueType_MainCanHaveDlq() {
        assertDoesNotThrow {
            Checks.checkQueueType(QueueType.MAIN, QueueOptions(dlqOptions = DlqOptions.DEFAULT))
        }
    }

    @Test
    fun testCheckQueueType_MainCanHaveArchive() {
        assertDoesNotThrow {
            Checks.checkQueueType(QueueType.MAIN, QueueOptions(archiveQueueOptions = ArchiveQueueOptions.DEFAULT))
        }
    }

    @Test
    fun testCheckQueueType_DlqCannotHaveDlq() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueType(QueueType.DLQ, QueueOptions(dlqOptions = DlqOptions.DEFAULT))
        }
    }

    @Test
    fun testCheckQueueType_ArchiveCannotHaveArchive() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueType(QueueType.ARCHIVE, QueueOptions(archiveQueueOptions = ArchiveQueueOptions.DEFAULT))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckDlqRetention_TooShort() {
        assertThrows<IllegalStateException> {
            Checks.checkDlqRetention(DlqOptions.MIN_RETENTION.minusNanos(1))
        }
    }

    @Test
    fun testCheckDlqRetention_TooLong() {
        assertThrows<IllegalStateException> {
            Checks.checkDlqRetention(DlqOptions.MAX_RETENTION.plusNanos(1))
        }
    }

    @Test
    fun testCheckDlqRetention_Valid() {
        assertDoesNotThrow { Checks.checkDlqRetention(DlqOptions.MIN_RETENTION) }
        assertDoesNotThrow { Checks.checkDlqRetention(DlqOptions.MAX_RETENTION) }
    }

    @Test
    fun testCheckArchiveQueueRetention_TooShort() {
        assertThrows<IllegalStateException> {
            Checks.checkArchiveQueueRetention(ArchiveQueueOptions.MIN_RETENTION.minusNanos(1))
        }
    }

    @Test
    fun testCheckArchiveQueueRetention_TooLong() {
        assertThrows<IllegalStateException> {
            Checks.checkArchiveQueueRetention(ArchiveQueueOptions.MAX_RETENTION.plusNanos(1))
        }
    }

    @Test
    fun testCheckArchiveQueueRetention_Valid() {
        assertDoesNotThrow { Checks.checkArchiveQueueRetention(ArchiveQueueOptions.MIN_RETENTION) }
        assertDoesNotThrow { Checks.checkArchiveQueueRetention(ArchiveQueueOptions.MAX_RETENTION) }
    }

    @Test
    fun testCheckRetentionMaxMessages_Positive() {
        assertDoesNotThrow { Checks.checkRetentionMaxMessages(1) }
        assertDoesNotThrow { Checks.checkRetentionMaxMessages(null) }
    }

    @Test
    fun testCheckRetentionMaxMessages_ZeroOrNegative() {
        assertThrows<IllegalStateException> { Checks.checkRetentionMaxMessages(0) }
        assertThrows<IllegalStateException> { Checks.checkRetentionMaxMessages(-1) }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckUserDefinedMetaFieldName_CannotEndWithReservedSuffix() {
        assertThrows<IllegalStateException> {
            Checks.checkUserDefinedMetaFieldName("field_dlq")
        }
        assertThrows<IllegalStateException> {
            Checks.checkUserDefinedMetaFieldName("field_arc")
        }
    }

    @Test
    fun testCheckUserDefinedMetaFieldName_ValidNames() {
        assertDoesNotThrow { Checks.checkUserDefinedMetaFieldName("user_id") }
        assertDoesNotThrow { Checks.checkUserDefinedMetaFieldName("field") }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckMetaFieldName_IfEmpty() {
        assertThrows<IllegalStateException> {
            Checks.checkMetaFieldName("")
        }
    }

    @Test
    fun testCheckMetaFieldName_TooLong() {
        val longName = "a".repeat(Const.META_FIELD_NAME_MAX_LENGTH + 1)
        assertThrows<IllegalStateException> {
            Checks.checkMetaFieldName(longName)
        }
    }

    @Test
    fun testCheckMetaFieldName_InvalidSymbols() {
        assertThrows<IllegalStateException> {
            Checks.checkMetaFieldName("meta$")
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckSweepMaxMessages_LessThanMin() {
        assertThrows<IllegalStateException> {
            Checks.checkSweepMaxMessages(SweepConfig.MIN_SWEEP_MESSAGES - 1)
        }
    }

    @Test
    fun testCheckSweepMaxMessages_MoreThanMax() {
        assertThrows<IllegalStateException> {
            Checks.checkSweepMaxMessages(SweepConfig.MAX_SWEEP_MESSAGES + 1)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckSweepProbability_LessThanMin() {
        assertThrows<IllegalStateException> {
            Checks.checkSweepProbability(SweepConfig.MIN_SWEEP_PROBABILITY - Math.ulp(SweepConfig.MIN_SWEEP_PROBABILITY))
        }
    }

    @Test
    fun testCheckSweepPeriod_MoreThanMax() {
        assertThrows<IllegalStateException> {
            Checks.checkSweepProbability(SweepConfig.MAX_SWEEP_PROBABILITY + Math.ulp(SweepConfig.MAX_SWEEP_PROBABILITY))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckClusterStateUpdateInterval() {
        assertThrows<IllegalStateException> {
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
        assertThrows<IllegalStateException> { Checks.checkSamplePercent(0.0f) }
        assertThrows<IllegalStateException> { Checks.checkSamplePercent(-1.0f) }
        assertThrows<IllegalStateException> { Checks.checkSamplePercent(100.01f) }
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
        assertThrows<IllegalStateException> {
            Checks.checkMutations(listOf(AddRemainingAttempts(1), SetRemainingAttempts(2)))
        }

        // Only scheduled_at field mutations
        assertThrows<IllegalStateException> {
            Checks.checkMutations(listOf(AddScheduledAt(Duration.ZERO), SetScheduledAt(Duration.ZERO)))
        }

        // More than one field
        assertThrows<IllegalStateException> {
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
