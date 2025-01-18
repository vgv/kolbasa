package kolbasa.cluster


import kolbasa.AbstractPostgresqlTest
import kolbasa.cluster.schema.ShardSchema
import kolbasa.cluster.schema.ShardSchema.CONSUMER_NODE_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.PRODUCER_NODE_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.SHARD_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.SHARD_TABLE_NAME
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.pg.DatabaseExtensions.useStatement
import org.junit.jupiter.api.Test
import java.sql.Statement
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ShardSchemaTest : AbstractPostgresqlTest() {

    @Test
    fun testCreateShardTableAndFill_FromEmptyToFull() {
        ShardSchema.createShardTable(dataSource)

        // Table just created, should be empty
        val empty = ShardSchema.readShards(dataSource)
        assertTrue(empty.isEmpty())

        // Fill shard table (empty -> full)
        val nodes = listOf("node1", "node2", "node3")
        ShardSchema.fillShardTable(dataSource, nodes)
        val full = ShardSchema.readShards(dataSource)
        checkFullShardsTable(full, nodes)
    }

    @Test
    fun testCreateShardTableAndFill_TestShardsOutOfBounds() {
        val nodes = listOf("node1", "node2", "node3")
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes)

        // insert additional shards with invalid shard numbers
        val shardsBeforeInsert = dataSource.readInt("select count(*) from $SHARD_TABLE_NAME")
        assertEquals(Shard.SHARD_COUNT, shardsBeforeInsert)
        dataSource.useStatement { statement: Statement ->
            val sql = """
                insert into $SHARD_TABLE_NAME
                    ($SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME)
                values
                    (${Shard.MAX_SHARD + 1}, 'a', 'a')
                on conflict do nothing
            """.trimIndent()
            statement.execute(sql)
        }
        val shardsAfterInsert = dataSource.readInt("select count(*) from $SHARD_TABLE_NAME")
        assertEquals(Shard.SHARD_COUNT + 1, shardsAfterInsert)

        // Read shard table and check that
        // 1) Shard with invalid number wasn't read
        // 2) All other shards were read
        val full = ShardSchema.readShards(dataSource)
        checkFullShardsTable(full, nodes)
    }

    @Test
    fun testCreateShardTableAndFill_FromNotSoEmptyToFull() {
        val nodes = listOf("node1", "node2", "node3")
        ShardSchema.createShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes)

        // Check that shards table is full
        val full = ShardSchema.readShards(dataSource)
        checkFullShardsTable(full, nodes)

        // Ok, let's delete some random shards and try to fill them again
        val shardsToDelete = (Shard.MIN_SHARD..Shard.MAX_SHARD)
            .toList()
            .shuffled()
            .take(Random.nextInt(50, 150))
            .toSet()
        dataSource.useStatement { statement: Statement ->
            val sql = """
                delete from $SHARD_TABLE_NAME
                where $SHARD_COLUMN_NAME in (${shardsToDelete.joinToString(separator = ",")})
            """.trimIndent()
            statement.execute(sql)
        }
        // check that shards were really deleted
        val notSoFull = ShardSchema.readShards(dataSource)
        assertEquals(Shard.SHARD_COUNT - shardsToDelete.size, notSoFull.size, "Deleted shards: $shardsToDelete, remaining: ${notSoFull.keys}")
        assertEquals(0, notSoFull.keys.intersect(shardsToDelete).size, "Deleted shards: $shardsToDelete, remaining: ${notSoFull.keys}")

        // Fill deleted shards and check that shards table is full again
        ShardSchema.fillShardTable(dataSource, nodes)
        val fullAfterDelete = ShardSchema.readShards(dataSource)
        checkFullShardsTable(fullAfterDelete, nodes)
    }

    private fun checkFullShardsTable(shards: Map<Int, Shard>, nodes: List<String>) {
        assertEquals(Shard.SHARD_COUNT, shards.size)

        (Shard.MIN_SHARD..Shard.MAX_SHARD).forEach { shardNumber ->
            val shard = requireNotNull(shards[shardNumber])
            assertEquals(shardNumber, shard.shard, "Shard: $shard")
            assertEquals(shard.producerNode, shard.consumerNode, "Shard: $shard")
            assertNull(shard.nextConsumerNode, "Shard: $shard")
            assertTrue(nodes.contains(shard.producerNode), "Shard: $shard")
            assertTrue(nodes.contains(shard.consumerNode), "Shard: $shard")
        }
    }

}
