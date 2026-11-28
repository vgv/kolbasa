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

        // Together with other nodes it is not, unless it gets every shard
        val admin = initializedCluster()
        assertEquals(listOf(NODE_02), admin.planInitShards().unsafeNodes)
        assertEquals(listOf(NODE_02), admin.planInitShards(NODE_01).unsafeNodes)
        assertEquals(emptyList<NodeId>(), admin.planInitShards(NODE_02).unsafeNodes)

        // Once the cluster has a map, messages on its nodes are normal: a shard that has lost its row everywhere is no risk
        admin.initShards()
        dataSources().forEach { node -> deleteShards(node, "5") }
        val plan = admin.planInitShards()
        assertEquals(setOf(ShardId.of(5)), plan.owners.keys)
        assertEquals(emptyList<NodeId>(), plan.unsafeNodes)
    }

    @Test
    fun testInitShards_Target_GetsEveryShardWithoutRow() {
        val admin = initializedCluster()

        val owners = admin.initShards(NODE_02)

        assertEquals(ShardId.SHARDS_RANGE.associate { ShardId.of(it) to NODE_02 }, owners)
        dataSources().forEach { node ->
            val shards = ShardSchema.readShards(node).values
            assertEquals(owners, shards.associate { it.shard to it.producerNode })
            assertTrue(shards.all { it.consumerNode == NODE_02 && it.revision == Instant.EPOCH })
        }

        // Every shard has a node now
        assertTrue(admin.planInitShards(NODE_03).isEmpty)
        assertEquals(emptyMap<ShardId, NodeId>(), admin.initShards(NODE_03))
    }

    @Test
    fun testInitShards_TargetAndShards() {
        val admin = initializedCluster()
        admin.initShards()
        dataSources().forEach { node -> deleteShards(node, "5, 6, 7") }

        // A shard listed twice is one shard
        val owners = admin.initShards(NODE_03, listOf(ShardId.of(5), ShardId.of(6), ShardId.of(5)))

        assertEquals(mapOf(ShardId.of(5) to NODE_03, ShardId.of(6) to NODE_03), owners)
        dataSources().forEach { node ->
            val shards = ShardSchema.readShards(node)
            assertEquals(NODE_03, shards[ShardId.of(5)]?.producerNode)
            assertEquals(NODE_03, shards[ShardId.of(6)]?.producerNode)
            // The shards that weren't listed stay without a row
            assertFalse(ShardId.of(7) in shards)
        }
    }

    @Test
    fun testInitShards_ShardWithRowIsRefused() {
        val admin = initializedCluster()
        admin.initShards()
        dataSources().forEach { node -> deleteShards(node, "5") }
        // Shard 700 has its row on one node only, and that is a node already
        deleteShards(dataSourceFirstSchema, "700")
        deleteShards(dataSourceSecondSchema, "700")
        val shards = listOf(ShardId.of(5), ShardId.of(700))

        val planned = assertThrows<ClusterOperationException.ShardsAlreadyInitializedException> {
            admin.planInitShards(NODE_03, shards)
        }
        val exception = assertThrows<ClusterOperationException.ShardsAlreadyInitializedException> {
            admin.initShards(NODE_03, shards)
        }

        assertEquals(listOf(ShardId.of(700)), planned.shards)
        assertEquals(listOf(ShardId.of(700)), exception.shards)
        assertTrue("Shard(s) 700 have a node already" in exception.message, exception.message)
        // No shard got a node: shard 5 has no row still
        dataSources().forEach { node -> assertFalse(ShardId.of(5) in ShardSchema.readShards(node)) }
    }

    @Test
    fun testInitShards_UnknownTargetIsRefused() {
        val admin = initializedCluster()
        val unknown = NodeId("node-99")

        assertThrows<ClusterOperationException.MoveToNonExistingNodeException> { admin.planInitShards(unknown) }
        val exception = assertThrows<ClusterOperationException.MoveToNonExistingNodeException> {
            admin.initShards(unknown)
        }

        assertEquals(unknown, exception.targetNode)
        dataSources().forEach { assertFalse(ShardSchema.shardTableExists(it)) }
    }

    @Test
    fun testInitShards_ReturnsWhatWasReadBack() {
        val admin = initializedCluster()
        // An application gives shard 5 to node-01 at the same moment: its row is on the node with the smallest id first.
        // Every new row of shard 5 on that node turns into that one
        ShardSchema.createOrUpdateShardTable(dataSource)
        dataSource.useStatement { statement ->
            statement.execute(
                "create function application_row() returns trigger language plpgsql as " +
                    "'begin if new.${ShardSchema.SHARD_COLUMN_NAME} = 5 then " +
                    "new.${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = ''node-01''; " +
                    "new.${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = ''node-01''; end if; return new; end'"
            )
            statement.execute(
                "create trigger application_row before insert on ${ShardSchema.SHARD_TABLE_NAME} " +
                    "for each row execute procedure application_row()"
            )
        }

        val owners = admin.initShards(NODE_03, listOf(ShardId.of(5), ShardId.of(6)))

        // Both rows have the initial revision, so the row of the node with the smallest id wins
        assertEquals(mapOf(ShardId.of(5) to NODE_01, ShardId.of(6) to NODE_03), owners)
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
