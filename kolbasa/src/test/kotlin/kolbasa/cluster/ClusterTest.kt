package kolbasa.cluster

import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.cluster.schema.ShardSchema
import kolbasa.producer.ShardId
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.IdSchema
import kolbasa.schema.NodeId
import kolbasa.utils.JdbcHelpers.readBoolean
import kolbasa.utils.JdbcHelpers.readInt
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource
import kotlin.random.Random

class ClusterTest : AbstractPostgreSQLTest() {

    @Test
    fun testInitCluster_If_No_DataSources() {
        val cluster = Cluster(emptyList(), emptyList())

        assertThrows<IllegalStateException> {
            cluster.updateTopology()
        }
    }

    @Test
    fun testInitCluster_Test_Topology_Init_And_Not() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val cluster = Cluster(dataSources, emptyList())

        // Topology not updated
        assertFalse(cluster.isInitialized())
        assertThrows<IllegalStateException> {
            cluster.getTopology()
        }

        // Update the topology
        cluster.updateTopology()

        // Topology updated
        assertTrue(cluster.isInitialized())
        assertDoesNotThrow {
            cluster.getTopology()
        }
    }

    @Test
    fun testInitCluster_If_Topology_The_Same() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

        val cluster = Cluster(dataSources, emptyList())

        // First initialization
        cluster.updateTopology()
        val firstTopology = cluster.getTopology()

        // Second initialization, check that topologies are the same
        cluster.updateTopology()
        val secondTopology = cluster.getTopology()

        assertSame(firstTopology, secondTopology)
        assertTrue(firstTopology.nodes.isNotEmpty(), "Topology: $firstTopology")
    }

    @Test
    fun testUpdateTopology_SameFingerprint_RowsNotReadAgain() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val countingNodes = dataSources.associateWith { CountingDataSource(it) }
        val cluster = Cluster(countingNodes.values.toList(), emptyList())

        // The map is seeded, and every node gets a copy
        cluster.updateTopology()
        val readsAfterSeeding = countingNodes.values.sumOf { it.shardTableReads.get() }

        // The map hasn't changed: no node is read, only their md5
        cluster.updateTopology()
        assertEquals(readsAfterSeeding, countingNodes.values.sumOf { it.shardTableReads.get() })

        // A hand edit on one node changes its md5, so only that node is read
        val editedNode = ClusterHelper.readNodes(dataSources).values.last()
        editedNode.useStatement { statement: Statement ->
            statement.execute(
                """
                update ${ShardSchema.SHARD_TABLE_NAME}
                set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-x', ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-x'
                where ${ShardSchema.SHARD_COLUMN_NAME} = 5
                """.trimIndent()
            )
        }
        cluster.updateTopology()
        assertEquals(readsAfterSeeding + 1, countingNodes.values.sumOf { it.shardTableReads.get() })
        assertEquals(NodeId("node-x"), cluster.getTopology().shards[ShardId.of(5)]?.producerNode)
    }

    @Test
    fun testInitCluster_If_Topology_Not_The_Same() {
        val dataSources = mapOf(
            "public" to dataSource,
            FIRST_SCHEMA_NAME to dataSourceFirstSchema,
            SECOND_SCHEMA_NAME to dataSourceSecondSchema
        )

        val cluster = Cluster(dataSources.values.toList(), emptyList())

        // First shard table initialization/read
        cluster.updateTopology()
        val firstTopology = cluster.getTopology()

        // ---------------------------------------------------------------------------------------
        // Make shard changes
        val shardToChange = ShardId.random()
        val currentProducerConsumerNode = requireNotNull(firstTopology.shards[shardToChange]?.producerNode)
        val newProducerConsumerNode = (firstTopology.nodes.keys - currentProducerConsumerNode).random()
        assertNotEquals(currentProducerConsumerNode, newProducerConsumerNode)

        // The map is seeded on the node with the smallest id
        val shardTableNode = ClusterHelper.readNodes(dataSources.values.toList()).values.first()
        shardTableNode.useStatement { statement: Statement ->
            val sql = """
                update
                    ${ShardSchema.SHARD_TABLE_NAME}
                set
                    ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = '${newProducerConsumerNode.id}',
                    ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = '${newProducerConsumerNode.id}'
                where
                    ${ShardSchema.SHARD_COLUMN_NAME} = ${shardToChange.id}
            """.trimIndent()
            statement.execute(sql)
        }
        // ---------------------------------------------------------------------------------------

        // Second shard table read
        cluster.updateTopology()
        val secondTopology = cluster.getTopology()

        assertNotEquals(firstTopology, secondTopology)
        assertEquals(secondTopology.shards[shardToChange]?.producerNode, newProducerConsumerNode)
        assertEquals(secondTopology.shards[shardToChange]?.consumerNode, newProducerConsumerNode)
    }

    @Test
    fun testInitCluster_All_Node_IDs_Initialized() {
        val dataSources = mapOf(
            "public" to dataSource,
            FIRST_SCHEMA_NAME to dataSourceFirstSchema,
            SECOND_SCHEMA_NAME to dataSourceSecondSchema
        )

        val cluster = Cluster(dataSources.values.toList(), emptyList())

        // First initialization
        cluster.updateTopology()
        val firstShardIDs = dataSources.mapNotNull { (_, dataSource) ->
            IdSchema.readNodeInfo(dataSource)
        }
        assertEquals(dataSources.size, firstShardIDs.size, "Data sources: $dataSources, shardIds: $firstShardIDs")

        // Second initialization, check that IDs are the same
        cluster.updateTopology()
        val secondShardIDs = dataSources.mapNotNull { (_, dataSource) ->
            IdSchema.readNodeInfo(dataSource)
        }
        assertEquals(dataSources.size, secondShardIDs.size, "Data sources: $dataSources, shardIds: $secondShardIDs")
        assertEquals(firstShardIDs, secondShardIDs)
    }

    @Test
    fun testInitCluster_If_No_Shard_Table_At_All() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()

        // A seeded map, every row with the initial revision, and every node holds a copy of it
        val shards = cluster.getTopology().shards
        assertEquals(ShardId.SHARD_COUNT, shards.size)
        assertTrue(shards.values.all { it.revision == Instant.EPOCH })
        assertEveryNodeHolds(dataSources, shards)
    }

    @Test
    fun testUpdateTopology_ShardsNoNodeHas_FilledOnTheSmallestNode() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val second = nodes.values.toList()[1]

        // An incomplete map on the second node, no map anywhere else
        ShardSchema.createOrUpdateShardTable(second)
        ShardSchema.fillShardTable(second, nodes.keys.toList())
        val before = ShardSchema.readShards(second)
        val deleted = deleteRandomShards(second)

        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()
        val shards = cluster.getTopology().shards
        assertEquals(ShardId.SHARD_COUNT, shards.size)

        // The shards that were there kept their nodes: only the missing ones were filled
        assertEquals(before - deleted, shards - deleted)

        // The missing ones got the initial revision, and every node holds the whole map
        assertTrue(shards.filterKeys { it in deleted }.values.all { it.revision == Instant.EPOCH })
        assertEveryNodeHolds(dataSources, shards)
    }

    @Test
    fun testUpdateTopology_TwoMapsWithTheSameRevision_EveryClientPicksTheSameRows() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, second, _) = nodes.values.toList()

        // Two clients seeded a new cluster at the same moment, each on its own smallest node: two random maps, both
        // with the initial revision
        ShardSchema.createOrUpdateShardTable(first)
        ShardSchema.fillShardTable(first, nodes.keys.toList())
        ShardSchema.createOrUpdateShardTable(second)
        ShardSchema.fillShardTable(second, nodes.keys.toList())

        // Two clients with different node lists, in a different order
        val oneClient = Cluster(dataSources, emptyList())
        oneClient.updateTopology()
        val anotherClient = Cluster(listOf(second, first), emptyList())
        anotherClient.updateTopology()

        // Both took the greater row of every shard, see Shard.compareTo
        val expected = ShardSchema.mergeShards(listOf(ShardSchema.readShards(first), ShardSchema.readShards(second)))
        assertEquals(expected, oneClient.getTopology().shards)
        assertEquals(expected, anotherClient.getTopology().shards)
    }

    @Test
    fun testUpdateTopology_MapOnOneNode_EveryNodeGetsACopy() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val second = nodes.values.toList()[1]

        // The map on one node only, as kolbasa kept it before every node had a copy
        ShardSchema.createOrUpdateShardTable(second)
        ShardSchema.fillShardTable(second, nodes.keys.toList())
        val map = ShardSchema.readShards(second)

        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()

        // The same rows, with the same revisions, on every node
        assertEquals(map, cluster.getTopology().shards)
        assertEveryNodeHolds(dataSources, map)
    }

    @Test
    fun testUpdateTopology_HandEditOnAnyNode_EveryNodeGetsIt() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()

        // An operator edits the copy on the last node
        val lastNode = ClusterHelper.readNodes(dataSources).values.last()
        lastNode.useStatement { statement: Statement ->
            statement.execute(
                """
                update ${ShardSchema.SHARD_TABLE_NAME}
                set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-x', ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-x'
                where ${ShardSchema.SHARD_COLUMN_NAME} = 17
                """.trimIndent()
            )
        }
        val editedRow = requireNotNull(ShardSchema.readShards(lastNode)[ShardId.of(17)])

        cluster.updateTopology()

        // The client uses the edit, and every node has it, with the revision the trigger gave it
        assertEquals(editedRow, cluster.getTopology().shards[ShardId.of(17)])
        assertEveryNodeHolds(dataSources, cluster.getTopology().shards)
    }

    @Test
    fun testUpdateTopology_LaggingCopies_CatchUp() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        val (first, second, third) = nodes.values.toList()

        // A cluster of two nodes for now, with an edit of shard 5 that both of them got
        val dataSources = mutableListOf(first, second)
        val cluster = Cluster({ dataSources.toList() }, { emptyList() })
        cluster.updateTopology()
        first.useStatement { statement: Statement ->
            statement.execute(
                "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-y', " +
                    "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-y' where ${ShardSchema.SHARD_COLUMN_NAME} = 5"
            )
        }
        cluster.updateTopology()
        val shard5 = requireNotNull(cluster.getTopology().shards[ShardId.of(5)])

        // The second node lags: the version of shard 5 from before the edit, as after a restore from a backup, and no
        // shard 7 at all
        second.useStatement { statement: Statement ->
            statement.execute("delete from ${ShardSchema.SHARD_TABLE_NAME} where ${ShardSchema.SHARD_COLUMN_NAME} in (5, 7)")
        }
        ShardSchema.writeShards(second, listOf(Shard(shard5.shard, NodeId("node-x"), NodeId("node-x"), null, Instant.EPOCH)))
        // ...and a new node joins, with no copy yet
        dataSources += third

        cluster.updateTopology()

        // The newer row and the deleted one are back, the new node has the whole map
        assertEquals(shard5, cluster.getTopology().shards[ShardId.of(5)])
        assertEveryNodeHolds(listOf(first, second, third), cluster.getTopology().shards)
    }

    @Test
    fun testUpdateTopology_NodeRejectsRows_NewTopologyInUseAndOtherNodesRepaired() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, middle, last) = nodes.values.toList()
        val middleNode = nodes.keys.toList()[1]

        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()

        // The middle node can't take any row, a node in read-only mode for example
        middle.useStatement { statement: Statement ->
            statement.execute(
                """
                create function reject_rows() returns trigger language plpgsql as ${'$'}${'$'}
                begin raise exception 'The node takes no rows'; end ${'$'}${'$'}
                """.trimIndent()
            )
            statement.execute(
                "create trigger reject_rows before insert or update on ${ShardSchema.SHARD_TABLE_NAME} " +
                    "for each row execute procedure reject_rows()"
            )
        }

        // An edit on the first node
        first.useStatement { statement: Statement ->
            statement.execute(
                "update ${ShardSchema.SHARD_TABLE_NAME} set ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = 'node-x', " +
                    "${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = 'node-x' where ${ShardSchema.SHARD_COLUMN_NAME} = 17"
            )
        }

        val exception = assertThrows<IllegalStateException> {
            cluster.updateTopology()
        }
        assertTrue(middleNode.id.id in exception.message.orEmpty(), exception.message)

        // The new topology is in use anyway, and the node after the failed one got the edit
        val editedRow = requireNotNull(ShardSchema.readShards(first)[ShardId.of(17)])
        assertEquals(editedRow, cluster.getTopology().shards[ShardId.of(17)])
        assertEquals(editedRow, ShardSchema.readShards(last)[ShardId.of(17)])
    }

    @Test
    fun testUpdateTopology_ShardTableReadFails_OldTopologyStays() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val secondNode = nodes.keys.toList()[1]
        val second = nodes.getValue(secondNode)

        // The map lives on the second node
        ShardSchema.createOrUpdateShardTable(second)
        ShardSchema.fillShardTable(second, nodes.keys.toList())

        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()
        val topology = cluster.getTopology()

        // The table is still there, but reading it fails
        second.useStatement { statement: Statement ->
            statement.execute(
                "alter table ${ShardSchema.SHARD_TABLE_NAME} rename column ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} to broken"
            )
        }

        val exception = assertThrows<IllegalStateException> {
            cluster.updateTopology()
        }
        assertTrue(secondNode.id.id in exception.message.orEmpty(), exception.message)

        // The old topology stays
        assertSame(topology, cluster.getTopology())
    }

    @Test
    fun testUpdateTopology_ShardTableCreatedAtTheSameMoment_SecondTryFindsIt() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val first = nodes.values.first()

        // Another client is creating the table on the first node right now and hasn't committed yet
        first.connection.use { otherClient ->
            otherClient.autoCommit = false
            otherClient.createStatement().use { statement ->
                statement.execute(ShardSchema.CREATE_SHARD_TABLE_STATEMENT)
            }

            val cluster = Cluster(dataSources, emptyList())
            val update = CompletableFuture.supplyAsync { cluster.updateTopology() }

            // Our create waits for the other one, then loses on the catalog once it commits
            waitUntil { dataSource.readInt(WAITING_SHARD_TABLE_CREATES) > 0 }
            otherClient.commit()

            update.get(30, TimeUnit.SECONDS)
            assertEquals(ShardId.SHARD_COUNT, cluster.getTopology().shards.size)
            assertEquals(ShardSchema.readShards(first), cluster.getTopology().shards)
        }
    }

    @Test
    fun testUpdateTopology_NodeAddedLater_GetsQueues() {
        val queue = Queue.of("test", PredefinedDataTypes.Int)
        var nodes = listOf(dataSource, dataSourceFirstSchema)
        val cluster = Cluster({ nodes }, { listOf(queue) })

        cluster.updateTopology()
        assertTrue(hasQueueTable(dataSourceFirstSchema, queue))
        assertFalse(hasQueueTable(dataSourceSecondSchema, queue))

        // A new node joins the cluster, and the next update creates the queue on it
        nodes = nodes + dataSourceSecondSchema
        cluster.updateTopology()

        assertTrue(hasQueueTable(dataSourceSecondSchema, queue))
    }

    @Test
    fun testUpdateTopology_QueueAddedLater_IsCreated() {
        val first = Queue.of("first", PredefinedDataTypes.Int)
        val second = Queue.of("second", PredefinedDataTypes.Int)
        val nodes = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        var queues = listOf(first)
        val cluster = Cluster({ nodes }, { queues })

        cluster.updateTopology()
        assertTrue(nodes.all { hasQueueTable(it, first) })
        assertTrue(nodes.none { hasQueueTable(it, second) })

        // A new queue appears, and the next update creates it on every node
        queues = queues + second
        cluster.updateTopology()

        assertTrue(nodes.all { hasQueueTable(it, second) })
    }

    @Test
    fun testUpdateTopology_FailedStatement_ReturnedAndRetried() {
        val queue = Queue.of("test", PredefinedDataTypes.Int)
        val cluster = Cluster(listOf(dataSourceFirstSchema, dataSourceSecondSchema), listOf(queue))

        // A table with the queue's name on the second node, but not a queue table. Kolbasa leaves it as is, and the
        // indexes on its missing columns fail on every try
        dataSourceSecondSchema.useStatement { statement: Statement -> statement.execute("create table q_test(x int)") }

        // Both nodes come back, the second one with its failures. Nothing throws, and the topology is in use anyway
        val failed = cluster.updateTopology()
        assertTrue(cluster.isInitialized())
        val first = requireNotNull(IdSchema.readNodeInfo(dataSourceFirstSchema)).id
        val second = requireNotNull(IdSchema.readNodeInfo(dataSourceSecondSchema)).id
        assertEquals(setOf(first, second), failed.nodes.keys)
        assertEquals(setOf(second), failed.failedNodes.keys)
        assertEquals(failed.nodes.getValue(second).failedStatements, failed.failedStatements)
        assertTrue(failed.failedStatements > 0)

        // Nothing has changed, but the next update runs the schema again: the failure is not forgotten
        assertEquals(setOf(second), cluster.updateTopology().failedNodes.keys)

        // The cause is gone, and the next update creates the queue
        dataSourceSecondSchema.useStatement { statement: Statement -> statement.execute("drop table q_test") }
        val fixed = cluster.updateTopology()
        assertEquals(0, fixed.failedStatements, "$fixed")
        assertEquals(setOf(first, second), fixed.nodes.keys)
        assertTrue(hasQueueTable(dataSourceSecondSchema, queue))

        // Now the schema is done, and an update with the same nodes and queues doesn't run it
        assertTrue(cluster.updateTopology().nodes.isEmpty())
    }

    // Every node holds exactly these rows, with these revisions
    private fun assertEveryNodeHolds(dataSources: List<DataSource>, shards: Map<ShardId, Shard>) {
        dataSources.forEach { dataSource ->
            assertEquals(shards, ShardSchema.readShards(dataSource))
            assertEquals(ShardSchema.fingerprint(shards.values), ShardSchema.readFingerprint(dataSource))
        }
    }

    // Deletes from 50 to 150 random shards, and returns them
    private fun deleteRandomShards(dataSource: DataSource): Set<ShardId> {
        val shards = ShardId.SHARDS_RANGE.shuffled().take(Random.nextInt(50, 150))
        dataSource.useStatement { statement: Statement ->
            statement.execute(
                """
                delete from ${ShardSchema.SHARD_TABLE_NAME}
                where ${ShardSchema.SHARD_COLUMN_NAME} in (${shards.joinToString(",")})
                """.trimIndent()
            )
        }
        return shards.map { ShardId.of(it) }.toSet()
    }

    private fun waitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
        while (!condition()) {
            check(System.nanoTime() < deadline) { "The condition wasn't met in 30 seconds" }
            Thread.sleep(20)
        }
    }

    private fun hasQueueTable(dataSource: DataSource, queue: Queue<*>): Boolean {
        return dataSource.readBoolean("select to_regclass('${queue.dbTableName}') is not null")
    }

    // A node that counts how many times the rows of q__shard are read from it
    private class CountingDataSource(private val delegate: DataSource) : DataSource by delegate {

        val shardTableReads = AtomicInteger()

        override fun getConnection(): Connection = CountingConnection(delegate.connection)

        private inner class CountingConnection(private val connection: Connection) : Connection by connection {
            override fun createStatement(): Statement = CountingStatement(connection.createStatement())
        }

        private inner class CountingStatement(private val statement: Statement) : Statement by statement {
            override fun executeQuery(sql: String): ResultSet {
                if (sql == ShardSchema.READ_SHARD_TABLE_STATEMENT) {
                    shardTableReads.incrementAndGet()
                }
                return statement.executeQuery(sql)
            }
        }
    }

    private companion object {
        // Creates of q__shard in this database that wait for a lock
        val WAITING_SHARD_TABLE_CREATES = """
            select count(*)
            from pg_stat_activity
            where
                datname = current_database() and
                wait_event_type = 'Lock' and
                position('create table if not exists ${ShardSchema.SHARD_TABLE_NAME}' in query) > 0
        """.trimIndent()
    }

}
