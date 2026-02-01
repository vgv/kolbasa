package kolbasa.producer

import kolbasa.cluster.Shard
import kolbasa.cluster.ShardStrategy
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors
import kotlin.math.abs

class ProducerSchemaHelpersTest {

    @Test
    fun testCalculateProducerName() {
        // Test all fourth combinations of producer names from ProducerOptions and SendOptions

        assertEquals(
            "send",
            ProducerSchemaHelpers.calculateProducerName(
                ProducerOptions(producer = "producer"),
                SendOptions(producer = "send")
            )
        )

        assertEquals(
            "producer",
            ProducerSchemaHelpers.calculateProducerName(
                ProducerOptions(producer = "producer"),
                SendOptions(producer = null)
            )
        )

        assertEquals(
            "send",
            ProducerSchemaHelpers.calculateProducerName(
                ProducerOptions(producer = null),
                SendOptions(producer = "send")
            )
        )

        assertEquals(
            null,
            ProducerSchemaHelpers.calculateProducerName(
                ProducerOptions(producer = null),
                SendOptions(producer = null)
            )
        )
    }

    @Test
    fun testCalculateDeduplicationMode_SendOptionsDefined() {
        val sendOptions = SendOptions(deduplicationMode = DeduplicationMode.FAIL_ON_DUPLICATE)
        val producerOptions = ProducerOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATE)

        val deduplicationMode = ProducerSchemaHelpers.calculateDeduplicationMode(producerOptions, sendOptions)
        assertEquals(DeduplicationMode.FAIL_ON_DUPLICATE, deduplicationMode)
    }

    @Test
    fun testCalculateDeduplicationMode_SendOptionsNotDefined() {
        val sendOptions = SendOptions.DEFAULT
        val producerOptions = ProducerOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATE)

        val deduplicationMode = ProducerSchemaHelpers.calculateDeduplicationMode(producerOptions, sendOptions)
        assertEquals(DeduplicationMode.IGNORE_DUPLICATE, deduplicationMode)
    }

    @Test
    fun testCalculateBatchSize_SendOptionsDefined() {
        val sendOptions = SendOptions(batchSize = 100)
        val producerOptions = ProducerOptions(batchSize = 200)

        val batchSize = ProducerSchemaHelpers.calculateBatchSize(producerOptions, sendOptions)
        assertEquals(100, batchSize)
    }

    @Test
    fun testCalculateBatchSize_SendOptionsNotDefined() {
        val sendOptions = SendOptions.DEFAULT
        val producerOptions = ProducerOptions(batchSize = 200)

        val batchSize = ProducerSchemaHelpers.calculateBatchSize(producerOptions, sendOptions)
        assertEquals(200, batchSize)
    }

    @Test
    fun testCalculatePartialInsert_SendOptionsDefined() {
        val sendOptions = SendOptions(partialInsert = PartialInsert.UNTIL_FIRST_FAILURE)
        val producerOptions = ProducerOptions(partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)

        val partialInsert = ProducerSchemaHelpers.calculatePartialInsert(producerOptions, sendOptions)
        assertEquals(PartialInsert.UNTIL_FIRST_FAILURE, partialInsert)
    }

    @Test
    fun testCalculatePartialInsert_SendOptionsNotDefined() {
        val sendOptions = SendOptions.DEFAULT
        val producerOptions = ProducerOptions(partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)

        val partialInsert = ProducerSchemaHelpers.calculatePartialInsert(producerOptions, sendOptions)
        assertEquals(PartialInsert.INSERT_AS_MANY_AS_POSSIBLE, partialInsert)
    }

    @Test
    fun testCalculateEffectiveShard_SendOptionsShardDefined() {
        val sendOptions = SendOptions(shard = -10000)
        val producerOptions = ProducerOptions(shard = -20000)
        val shardStrategy = ShardStrategy.Fixed(-30000)

        val effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(sendOptions, producerOptions, shardStrategy)
        assertTrue(effectiveShard in Shard.MIN_SHARD..Shard.MAX_SHARD, "effectiveShard=$effectiveShard")
        assertEquals(abs(-10000 % Shard.SHARD_COUNT), effectiveShard)
    }

    @Test
    fun testCalculateEffectiveShard_ProducerOptionsShardDefined() {
        val sendOptions = SendOptions(shard = null)
        val producerOptions = ProducerOptions(shard = -20000)
        val shardStrategy = ShardStrategy.Fixed(-30000)

        val effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(sendOptions, producerOptions, shardStrategy)
        assertTrue(effectiveShard in Shard.MIN_SHARD..Shard.MAX_SHARD, "effectiveShard=$effectiveShard")
        assertEquals(abs(-20000 % Shard.SHARD_COUNT), effectiveShard)
    }

    @Test
    fun testCalculateEffectiveShard_ShardStrategyDefined() {
        val sendOptions = SendOptions(shard = null)
        val producerOptions = ProducerOptions(shard = null)
        val shardStrategy = ShardStrategy.Fixed(-30000)

        val effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(sendOptions, producerOptions, shardStrategy)
        assertTrue(effectiveShard in Shard.MIN_SHARD..Shard.MAX_SHARD, "effectiveShard=$effectiveShard")
        assertEquals(abs(-30000 % Shard.SHARD_COUNT), effectiveShard)
    }

    @Test
    fun testCalculateAsyncExecutor() {
        val callExecutor = Executors.newCachedThreadPool()
        val producerExecutor = Executors.newCachedThreadPool()
        val defaultExecutor = Executors.newCachedThreadPool()

        assertSame(defaultExecutor, ProducerSchemaHelpers.calculateAsyncExecutor(null, null, defaultExecutor))
        assertSame(producerExecutor, ProducerSchemaHelpers.calculateAsyncExecutor(null, producerExecutor, defaultExecutor))
        assertSame(callExecutor, ProducerSchemaHelpers.calculateAsyncExecutor(callExecutor, null, defaultExecutor))
        assertSame(callExecutor, ProducerSchemaHelpers.calculateAsyncExecutor(callExecutor, producerExecutor, defaultExecutor))
    }

}
