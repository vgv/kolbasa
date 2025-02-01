package kolbasa.producer

import kolbasa.cluster.Shard
import kolbasa.cluster.ShardStrategy
import java.util.concurrent.Executors
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

class ProducerSchemaHelpersTest {

    @Test
    fun testCalculateDeduplicationMode_SendOptionsDefined() {
        val sendOptions = SendOptions(deduplicationMode = DeduplicationMode.ERROR)
        val producerOptions = ProducerOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATES)

        val deduplicationMode = ProducerSchemaHelpers.calculateDeduplicationMode(producerOptions, sendOptions)
        assertEquals(DeduplicationMode.ERROR, deduplicationMode)
    }

    @Test
    fun testCalculateDeduplicationMode_SendOptionsNotDefined() {
        val sendOptions = SendOptions.SEND_OPTIONS_NOT_SET
        val producerOptions = ProducerOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATES)

        val deduplicationMode = ProducerSchemaHelpers.calculateDeduplicationMode(producerOptions, sendOptions)
        assertEquals(DeduplicationMode.IGNORE_DUPLICATES, deduplicationMode)
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
        val sendOptions = SendOptions.SEND_OPTIONS_NOT_SET
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
        val sendOptions = SendOptions.SEND_OPTIONS_NOT_SET
        val producerOptions = ProducerOptions(partialInsert = PartialInsert.INSERT_AS_MANY_AS_POSSIBLE)

        val partialInsert = ProducerSchemaHelpers.calculatePartialInsert(producerOptions, sendOptions)
        assertEquals(PartialInsert.INSERT_AS_MANY_AS_POSSIBLE, partialInsert)
    }

    @Test
    fun testCalculateEffectiveShard_SendOptionsShardDefined() {
        val sendOptions = SendOptions(shard = -10000)
        val producerOptions = ProducerOptions(shard = -20000)
        val shardStrategy = ShardStrategy.Fixed(-30000)

        val effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(producerOptions, sendOptions, shardStrategy)
        assertTrue(effectiveShard in Shard.MIN_SHARD..Shard.MAX_SHARD, "effectiveShard=$effectiveShard")
        assertEquals(abs(-10000 % Shard.SHARD_COUNT), effectiveShard)
    }

    @Test
    fun testCalculateEffectiveShard_ProducerOptionsShardDefined() {
        val sendOptions = SendOptions(shard = null)
        val producerOptions = ProducerOptions(shard = -20000)
        val shardStrategy = ShardStrategy.Fixed(-30000)

        val effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(producerOptions, sendOptions, shardStrategy)
        assertTrue(effectiveShard in Shard.MIN_SHARD..Shard.MAX_SHARD, "effectiveShard=$effectiveShard")
        assertEquals(abs(-20000 % Shard.SHARD_COUNT), effectiveShard)
    }

    @Test
    fun testCalculateEffectiveShard_ShardStrategyDefined() {
        val sendOptions = SendOptions(shard = null)
        val producerOptions = ProducerOptions(shard = null)
        val shardStrategy = ShardStrategy.Fixed(-30000)

        val effectiveShard = ProducerSchemaHelpers.calculateEffectiveShard(producerOptions, sendOptions, shardStrategy)
        assertTrue(effectiveShard in Shard.MIN_SHARD..Shard.MAX_SHARD, "effectiveShard=$effectiveShard")
        assertEquals(abs(-30000 % Shard.SHARD_COUNT), effectiveShard)
    }

    @Test
    fun testCalculateAsyncExecutor_IfDefined() {
        val customExecutor = Executors.newCachedThreadPool()
        val defaultExecutor = Executors.newCachedThreadPool()
        val producerOptions = ProducerOptions(asyncExecutor = customExecutor)

        val asyncExecutor = ProducerSchemaHelpers.calculateAsyncExecutor(producerOptions, defaultExecutor)
        assertSame(customExecutor, asyncExecutor)
    }

    @Test
    fun testCalculateAsyncExecutor_IfNotDefined() {
        val defaultExecutor = Executors.newCachedThreadPool()
        val producerOptions = ProducerOptions(asyncExecutor = null)

        val asyncExecutor = ProducerSchemaHelpers.calculateAsyncExecutor(producerOptions, defaultExecutor)
        assertSame(defaultExecutor, asyncExecutor)
    }
}
