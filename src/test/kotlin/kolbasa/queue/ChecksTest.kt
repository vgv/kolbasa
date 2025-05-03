package kolbasa.queue

import kolbasa.cluster.ClusterStateUpdateConfig
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.AddScheduledAt
import kolbasa.mutator.SetRemainingAttempts
import kolbasa.mutator.SetScheduledAt
import kolbasa.schema.Const
import kolbasa.stats.prometheus.PrometheusConfig
import java.time.Duration
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotSame

internal class ChecksTest {

    @Test
    fun testCheckDelay_DelayNotSetWorks() {
        // check DELAY_NOT_SET doesn't fail
        Checks.checkDelay(QueueOptions.DELAY_NOT_SET)
    }

    @Test
    fun testCheckDelay_DelayNotSetCopyFails() {
        // Check any copies of DELAY_NOT_SET doesn't work
        val copy = QueueOptions.DELAY_NOT_SET.negated().negated()
        assertNotSame(copy, QueueOptions.DELAY_NOT_SET)
        assertEquals(copy, QueueOptions.DELAY_NOT_SET)
        assertFailsWith<IllegalStateException> {
            Checks.checkDelay(copy)
        }
    }

    @Test
    fun testCheckDelay_ZeroOrPositiveWorks() {
        //Check all values >= zero work
        Checks.checkDelay(Duration.ofMillis(0))
        Checks.checkDelay(Duration.ofMillis(Random.nextLong(1, Long.MAX_VALUE)))
    }

    @Test
    fun testCheckDelay_NegativeFails() {
        // Check other negative values fail
        assertFailsWith<IllegalStateException> {
            Checks.checkDelay(Duration.ofMillis(Random.nextLong(Long.MIN_VALUE, 0)))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckAttempts_AttemptsNotSetWorks() {
        // check DELAY_NOT_SET works
        Checks.checkAttempts(QueueOptions.ATTEMPTS_NOT_SET)
    }

    @Test
    fun testCheckAttempts_PositiveWorks() {
        //Check all positive values work
        Checks.checkAttempts(Random.nextInt(1, Int.MAX_VALUE))
    }

    @Test
    fun testCheckAttempts_ZeroOrNegativeFails() {
        // Check other negative or zero values fail
        assertFailsWith<IllegalStateException> {
            Checks.checkAttempts(0)
        }
        assertFailsWith<IllegalStateException> {
            Checks.checkAttempts(Random.nextInt(Int.MIN_VALUE, 0))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckProducerName() {
        Checks.checkProducerName(null)
        Checks.checkProducerName("just value shorter than 255 symbols")

        assertFailsWith<IllegalStateException> {
            val longValue = "a".repeat(Const.PRODUCER_CONSUMER_VALUE_LENGTH + 1)
            Checks.checkProducerName(longValue)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testBatchSize_ZeroOrNegativeFails() {
        assertFailsWith<IllegalStateException> {
            Checks.checkBatchSize(0)
        }
        assertFailsWith<IllegalStateException> {
            Checks.checkBatchSize(-1)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckConsumerName() {
        Checks.checkConsumerName(null)
        Checks.checkConsumerName("just value shorter than 255 symbols")

        assertFailsWith<IllegalStateException> {
            val longValue = "a".repeat(Const.PRODUCER_CONSUMER_VALUE_LENGTH + 1)
            Checks.checkConsumerName(longValue)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckVisibilityTimeout_VisibilityTimeoutNotSetWorks() {
        // check DELAY_NOT_SET works
        Checks.checkVisibilityTimeout(QueueOptions.VISIBILITY_TIMEOUT_NOT_SET)
    }

    @Test
    fun testCheckVisibilityTimeout_VisibilityTimeoutCopyFails() {
        // Check any copies of DELAY_NOT_SET fail
        val copy = QueueOptions.VISIBILITY_TIMEOUT_NOT_SET.negated().negated()
        assertNotSame(copy, QueueOptions.VISIBILITY_TIMEOUT_NOT_SET)
        assertEquals(copy, QueueOptions.VISIBILITY_TIMEOUT_NOT_SET)
        assertFailsWith<IllegalStateException> {
            Checks.checkVisibilityTimeout(copy)
        }
    }

    @Test
    fun testCheckVisibilityTimeout_ZeroOrPositiveWorks() {
        //Check all values >= zero work
        Checks.checkVisibilityTimeout(Duration.ofMillis(0))
        Checks.checkVisibilityTimeout(Duration.ofMillis(Random.nextLong(1, Long.MAX_VALUE)))
    }

    @Test
    fun testCheckVisibilityTimeout_NegativeFails() {
        // Check other negative values fail
        assertFailsWith<IllegalStateException> {
            Checks.checkVisibilityTimeout(Duration.ofMillis(Random.nextLong(Long.MIN_VALUE, 0)))
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckQueueName_IfEmpty() {
        assertFailsWith<IllegalStateException> {
            Checks.checkQueueName("")
        }
    }

    @Test
    fun testCheckQueueName_InvalidPrefix() {
        assertFailsWith<IllegalStateException> {
            Checks.checkQueueName("q_customer_email")
        }
    }

    @Test
    fun testCheckQueueName_TooLong() {
        val longName = "a".repeat(Const.QUEUE_NAME_MAX_LENGTH + 1)
        assertFailsWith<IllegalStateException> {
            Checks.checkQueueName(longName)
        }
    }

    @Test
    fun testCheckQueueName_InvalidSymbols() {
        assertFailsWith<IllegalStateException> {
            Checks.checkQueueName("queue$")
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckMetaFieldName_IfEmpty() {
        assertFailsWith<IllegalStateException> {
            Checks.checkMetaFieldName("")
        }
    }

    @Test
    fun testCheckMetaFieldName_TooLong() {
        val longName = "a".repeat(Const.META_FIELD_NAME_MAX_LENGTH + 1)
        assertFailsWith<IllegalStateException> {
            Checks.checkMetaFieldName(longName)
        }
    }

    @Test
    fun testCheckMetaFieldName_InvalidSymbols() {
        assertFailsWith<IllegalStateException> {
            Checks.checkMetaFieldName("meta$")
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckSweepMaxRows_LessThanMin() {
        assertFailsWith<IllegalStateException> {
            Checks.checkSweepMaxRows(SweepConfig.MIN_SWEEP_ROWS - 1)
        }
    }

    @Test
    fun testCheckSweepMaxRows_MoreThanMax() {
        assertFailsWith<IllegalStateException> {
            Checks.checkSweepMaxRows(SweepConfig.MAX_SWEEP_ROWS + 1)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckSweepMaxIterations_LessThanMin() {
        assertFailsWith<IllegalStateException> {
            Checks.checkSweepMaxIterations(SweepConfig.MIN_SWEEP_ITERATIONS - 1)
        }
    }

    @Test
    fun testCheckSweepMaxIterations_MoreThanMax() {
        assertFailsWith<IllegalStateException> {
            Checks.checkSweepMaxIterations(SweepConfig.MAX_SWEEP_ITERATIONS + 1)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckSweepPeriod_LessThanMin() {
        assertFailsWith<IllegalStateException> {
            Checks.checkSweepPeriod(SweepConfig.MIN_SWEEP_PERIOD - 1)
        }
    }

    @Test
    fun testCheckSweepPeriod_MoreThanMax() {
        assertFailsWith<IllegalStateException> {
            Checks.checkSweepPeriod(SweepConfig.MAX_SWEEP_PERIOD + 1)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckCustomQueueSizeMeasureInterval() {
        assertFailsWith<IllegalStateException> {
            val ulp = Duration.ofNanos(1)
            val aBitSmaller = PrometheusConfig.Config.MIN_QUEUE_SIZE_MEASURE_INTERVAL - ulp
            Checks.checkCustomQueueSizeMeasureInterval("some_queue", aBitSmaller)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testCheckClusterStateUpdateInterval() {
        assertFailsWith<IllegalStateException> {
            val ulp = Duration.ofNanos(1)
            val aBitSmaller = ClusterStateUpdateConfig.MIN_INTERVAL - ulp
            Checks.checkClusterStateUpdateInterval(aBitSmaller)
        }
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
        assertFailsWith<IllegalStateException> {
            Checks.checkMutations(listOf(AddRemainingAttempts(1), SetRemainingAttempts(2)))
        }

        // Only scheduled_at field mutations
        assertFailsWith<IllegalStateException> {
            Checks.checkMutations(listOf(AddScheduledAt(Duration.ZERO), SetScheduledAt(Duration.ZERO)))
        }

        // More than one field
        assertFailsWith<IllegalStateException> {
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
