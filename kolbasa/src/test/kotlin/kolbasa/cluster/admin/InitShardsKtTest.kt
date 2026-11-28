package kolbasa.cluster.admin

import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import javax.sql.DataSource

class InitShardsKtTest : AbstractPostgreSQLTest() {

    @Test
    fun testInitShards_NewCluster() {
        val admin = initializedCluster()

        val owners = admin.initShards()

        // The nodes take turns
        assertEquals(ShardId.SHARD_COUNT, owners.size)
        assertEquals(listOf(NODE_01, NODE_02, NODE_03, NODE_01), (0..3).map { owners.getValue(ShardId.of(it)) })
        // Every node has the whole map: the shards produced and consumed by their nodes, with the initial revision
        dataSources().forEach { node ->
            val shards = ShardSchema.readShards(node).values
            assertEquals(owners, shards.associate { it.shard to it.producerNode })
            assertTrue(shards.all { it.consumerNode == it.producerNode && it.nextConsumerNode == null })
            assertTrue(shards.all { it.revision == Instant.EPOCH })
        }
        assertTrue(admin.checkShardMap().isClean, "${admin.checkShardMap()}")

        // Nothing is left to do, and the copies are the same already
        assertTrue(admin.planInitShards().isEmpty)
        assertEquals(emptyMap<ShardId, NodeId>(), admin.initShards())
        assertEquals(mapOf(NODE_01 to 0, NODE_02 to 0, NODE_03 to 0), admin.syncShards())
    }

    @Test
    fun testInitShards_OnlyShardsNoNodeHas() {
        val admin = initializedCluster()
        admin.initShards()

        // No node has shards 5 and 6 any more, the third node has lost its table, and the first copy has an edit by hand:
        // shard 7 goes from node-02 to node-03
        dataSources().forEach { node -> deleteShards(node, "5, 6") }
        execute(dataSourceSecondSchema, "drop table ${ShardSchema.SHARD_TABLE_NAME}")
        execute(
            dataSource,
            "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-03', " +
                "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-03' where ${ShardSchema.SHARD_COLUMN_NAME} = 7"
        )

        val owners = admin.initShards()

        // node-02 has the fewest shards after the edit, then the three nodes have 341 each, and the smallest id wins the tie
        assertEquals(mapOf(ShardId.of(5) to NODE_02, ShardId.of(6) to NODE_01), owners)
        dataSources().forEach { node ->
            val shards = ShardSchema.readShards(node)
            assertEquals(NODE_02, shards[ShardId.of(5)]?.producerNode)
            assertEquals(NODE_01, shards[ShardId.of(6)]?.producerNode)
        }
        // The node without a table gets one, with the new rows only. The copies that differ stay as they are
        assertEquals(setOf(ShardId.of(5), ShardId.of(6)), ShardSchema.readShards(dataSourceSecondSchema).keys)
        assertEquals(NODE_03, ShardSchema.readShards(dataSource)[ShardId.of(7)]?.producerNode)
        assertEquals(NODE_02, ShardSchema.readShards(dataSourceFirstSchema)[ShardId.of(7)]?.producerNode)
    }

    @Test
    fun testInitShards_NodeRejectsRows_OtherNodesGetThem() {
        val admin = initializedCluster()
        // The middle node can't take any row
        ShardSchema.createOrUpdateShardTable(dataSourceFirstSchema)
        dataSourceFirstSchema.useStatement { statement ->
            statement.execute(
                "create function reject_rows() returns trigger language plpgsql as " +
                    "'begin raise exception ''The node takes no rows''; end'"
            )
            statement.execute(
                "create trigger reject_rows before insert or update on ${ShardSchema.SHARD_TABLE_NAME} " +
                    "for each row execute procedure reject_rows()"
            )
        }

        val exception = assertThrows<ClusterOperationException.ExecutionException> {
            admin.initShards()
        }

        assertTrue("on node ${NODE_02.id}" in exception.message, exception.message)
        assertTrue("it is in effect" in exception.message, exception.message)
        assertTrue("sync the shards" in exception.message, exception.message)
        // The nodes before and after the failed one have the rows
        assertEquals(ShardId.SHARD_COUNT, ShardSchema.readShards(dataSource).size)
        assertEquals(0, ShardSchema.readShards(dataSourceFirstSchema).size)
        assertEquals(ShardId.SHARD_COUNT, ShardSchema.readShards(dataSourceSecondSchema).size)
    }

    @Test
    fun testPlanInitShards_IsWhatInitShardsDoes() {
        val admin = initializedCluster()

        val plan = admin.planInitShards()

        assertEquals(ShardId.SHARD_COUNT, plan.owners.size)
        assertEquals(emptyList<NodeId>(), plan.unsafeNodes)
        assertFalse(plan.isEmpty)
        // The plan changes nothing
        dataSources().forEach { assertFalse(ShardSchema.shardTableExists(it)) }

        assertEquals(plan.owners, admin.initShards())
    }

    @Test
    fun testPlanInitShards_NodeWithMessages() {
        // A database used without a cluster, which has its id already: a queue with a message
        ClusterAdmin(mapOf(NODE_02 to dataSourceFirstSchema)).initNodes()
        SchemaHelpers.createOrUpdateQueues(dataSourceFirstSchema, QUEUE)
        DatabaseProducer(dataSourceFirstSchema).send(QUEUE, "message").throwExceptionIfAny()

        // Alone, it gets every shard, and that is safe
        assertEquals(emptyList<NodeId>(), ClusterAdmin(mapOf(NODE_02 to dataSourceFirstSchema)).planInitShards().unsafeNodes)

        // Together with other nodes it is not
        val admin = initializedCluster()
        assertEquals(listOf(NODE_02), admin.planInitShards().unsafeNodes)

        // Once the cluster has a map, messages on its nodes are normal: a shard that has lost its row everywhere is no risk
        admin.initShards()
        dataSources().forEach { node -> deleteShards(node, "5") }
        val plan = admin.planInitShards()
        assertEquals(setOf(ShardId.of(5)), plan.owners.keys)
        assertEquals(emptyList<NodeId>(), plan.unsafeNodes)
    }

    // ---------- helpers ----------

    // Three nodes with their ids, and no shard map yet
    private fun initializedCluster(): ClusterAdmin {
        val admin = ClusterAdmin(
            mapOf(NODE_01 to dataSource, NODE_02 to dataSourceFirstSchema, NODE_03 to dataSourceSecondSchema)
        )
        admin.initNodes()
        return admin
    }

    private fun dataSources(): List<DataSource> = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

    private fun deleteShards(dataSource: DataSource, shards: String) {
        execute(dataSource, "delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} in ($shards)")
    }

    private fun execute(dataSource: DataSource, sql: String) {
        dataSource.useStatement { statement -> statement.execute(sql) }
    }

    private companion object {
        val NODE_01 = NodeId("node-01")
        val NODE_02 = NodeId("node-02")
        val NODE_03 = NodeId("node-03")

        val QUEUE = Queue(
            name = "init_shards_test",
            databaseDataType = PredefinedDataTypes.String
        )
    }
}
