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
import java.sql.Statement
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
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

        val shardTables = findShardTables(dataSources).filter { foundShardTable ->
            foundShardTable.numberOfTables > 0
        }
        assertEquals(1, shardTables.size)
        shardTables.first().datasource.useStatement { statement: Statement ->
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
        val dataSources = mapOf(
            "public" to dataSource,
            FIRST_SCHEMA_NAME to dataSourceFirstSchema,
            SECOND_SCHEMA_NAME to dataSourceSecondSchema
        )

        val cluster = Cluster(dataSources.values.toList(), emptyList())
        cluster.updateTopology()

        // Number of created shard tables
        val shardTables = findShardTables(dataSources)
            .filter { foundShardTable ->
                foundShardTable.numberOfTables > 0
            }

        // Check that only one shard table was created
        assertEquals(1, shardTables.size)

        // Check that shard table was created on the node with the smallest id
        val nodeIdWithShardTable = shardTables.first().nodeId
        val smallestExistingId = dataSources
            .mapNotNull { (_, dataSource) ->
                IdSchema.readNodeInfo(dataSource)
            }
            .minOf { it }
        assertEquals(smallestExistingId.id, nodeIdWithShardTable)
    }

    @Test
    fun testUpdateTopology_IncompleteShardTable_FilledInPlace() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, second, third) = nodes.values.toList()

        // An incomplete table on the second node, not on the one with the smallest id
        ShardSchema.createOrUpdateShardTable(second)
        ShardSchema.fillShardTable(second, nodes.keys.toList())
        val before = ShardSchema.readShards(second)
        val deleted = deleteRandomShards(second)

        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()

        // The table was filled where it is, no new table appeared next to it
        assertFalse(ShardSchema.shardTableExists(first))
        assertFalse(ShardSchema.shardTableExists(third))
        val after = ShardSchema.readShards(second)
        assertEquals(ShardId.SHARD_COUNT, after.size)
        assertEquals(after, cluster.getTopology().shards)

        // The shards that were there kept their nodes
        assertEquals(before - deleted, after - deleted)
    }

    @Test
    fun testUpdateTopology_CompleteShardTable_WinsOverIncomplete() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (first, second, _) = nodes.values.toList()

        // An incomplete table on the first node, a complete one on the second
        ShardSchema.createOrUpdateShardTable(first)
        ShardSchema.fillShardTable(first, nodes.keys.toList())
        val deleted = deleteRandomShards(first)
        ShardSchema.createOrUpdateShardTable(second)
        ShardSchema.fillShardTable(second, nodes.keys.toList())

        val cluster = Cluster(dataSources, emptyList())
        cluster.updateTopology()

        // The complete table is used, the incomplete one is left as it is
        assertEquals(ShardSchema.readShards(second), cluster.getTopology().shards)
        assertEquals(ShardId.SHARD_COUNT - deleted.size, ShardSchema.readShards(first).size)
    }

    @Test
    fun testUpdateTopology_ShardTableReadFails_OldTopologyStays() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val nodes = ClusterHelper.readNodes(dataSources)
        val (firstNode, secondNode, _) = nodes.keys.toList()
        val (first, second, third) = nodes.values.toList()

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

        // No new map anywhere, and the old topology stays
        assertFalse(ShardSchema.shardTableExists(first), "Node ${firstNode.id.id}")
        assertFalse(ShardSchema.shardTableExists(third))
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

    private fun findShardTables(dataSources: Map<String, DataSource>): List<FoundShardTable> {
        return dataSources
            .map { (schema, dataSource) ->
                val sql = """
                    select
                        count(*)
                    from
                        information_schema.tables
                    where
                        table_schema='$schema' and
                        table_name='${ShardSchema.SHARD_TABLE_NAME}'
                """.trimIndent()

                val numberOfTables = dataSource.readInt(sql)
                val thisNodeId = requireNotNull(IdSchema.readNodeInfo(dataSource))
                FoundShardTable(numberOfTables, thisNodeId.id, schema, dataSource)
            }
    }

    private data class FoundShardTable(
        val numberOfTables: Int,
        val nodeId: NodeId,
        val schema: String,
        val datasource: DataSource
    )

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
