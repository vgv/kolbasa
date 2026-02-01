package kolbasa.queue

import kolbasa.cluster.ClusterStateUpdateConfig
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.AddScheduledAt
import kolbasa.mutator.SetRemainingAttempts
import kolbasa.mutator.SetScheduledAt
import kolbasa.schema.Const
import kolbasa.stats.prometheus.PrometheusConfig
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
            Checks.checkQueueName("")
        }
    }

    @Test
    fun testCheckQueueName_InvalidPrefix() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("q_customer_email")
        }
    }

    @Test
    fun testCheckQueueName_TooLong() {
        val longName = "a".repeat(Const.QUEUE_NAME_MAX_LENGTH + 1)
        assertThrows<IllegalStateException> {
            Checks.checkQueueName(longName)
        }
    }

    @Test
    fun testCheckQueueName_InvalidSymbols() {
        assertThrows<IllegalStateException> {
            Checks.checkQueueName("queue$")
        }
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
    fun testCheckCustomQueueSizeMeasureInterval() {
        assertThrows<IllegalStateException> {
            val ulp = Duration.ofNanos(1)
            val aBitSmaller = PrometheusConfig.Config.MIN_QUEUE_SIZE_MEASURE_INTERVAL - ulp
            Checks.checkCustomQueueSizeMeasureInterval("some_queue", aBitSmaller)
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
