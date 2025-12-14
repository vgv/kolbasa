package kolbasa.queue

import kolbasa.cluster.ClusterStateUpdateConfig
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.AddScheduledAt
import kolbasa.mutator.SetRemainingAttempts
import kolbasa.mutator.SetScheduledAt
import kolbasa.schema.Const
import kolbasa.stats.prometheus.PrometheusConfig
import org.junit.jupiter.api.assertDoesNotThrow
import java.time.Duration
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith

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
        assertFailsWith<IllegalStateException> {
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

        // too long name
        assertFailsWith<IllegalStateException> {
            val longName = "a".repeat(Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH + 1)
            Checks.checkProducerName(longName)
        }

        // wrong symbols
        assertFailsWith<IllegalStateException> {
            val wrongName = "producer;name"
            Checks.checkProducerName(wrongName)
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    fun testBatchSize_ZeroOrNegative_Fails() {
        assertFailsWith<IllegalStateException> {
            Checks.checkBatchSize(0)
        }
        assertFailsWith<IllegalStateException> {
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
        assertFailsWith<IllegalStateException> {
            val longName = "a".repeat(Const.PRODUCER_CONSUMER_VALUE_MAX_LENGTH + 1)
            Checks.checkConsumerName(longName)
        }

        // wrong symbols
        assertFailsWith<IllegalStateException> {
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
    fun testCheckSweepMaxMessages_LessThanMin() {
        assertFailsWith<IllegalStateException> {
            Checks.checkSweepMaxMessages(SweepConfig.MIN_SWEEP_MESSAGES - 1)
        }
    }

    @Test
    fun testCheckSweepMaxMessages_MoreThanMax() {
        assertFailsWith<IllegalStateException> {
            Checks.checkSweepMaxMessages(SweepConfig.MAX_SWEEP_MESSAGES + 1)
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
