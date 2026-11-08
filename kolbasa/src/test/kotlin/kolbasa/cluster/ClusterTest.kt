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
import javax.sql.DataSource

class ClusterTest : AbstractPostgreSQLTest() {

    @Test
    fun testInitCluster_If_No_DataSources() {
        val cluster = Cluster(emptyList())

        assertThrows<IllegalStateException> {
            cluster.updateTopology()
        }
    }

    @Test
    fun testInitCluster_Test_Topology_Init_And_Not() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val cluster = Cluster(dataSources)

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

        val cluster = Cluster(dataSources)

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

        val cluster = Cluster(dataSources.values.toList())

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

        val cluster = Cluster(dataSources.values.toList())

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

        val cluster = Cluster(dataSources.values.toList())
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
    fun testUpdateTopology_NodeAddedLater_GetsQueues() {
        val queue = Queue.of("test", PredefinedDataTypes.Int)
        var nodes = listOf(dataSource, dataSourceFirstSchema)
        val cluster = Cluster({ nodes }, listOf(queue))

        cluster.updateTopology()
        assertTrue(hasQueueTable(dataSourceFirstSchema, queue))
        assertFalse(hasQueueTable(dataSourceSecondSchema, queue))

        // A new node joins the cluster, and the next update creates the queue on it
        nodes = nodes + dataSourceSecondSchema
        cluster.updateTopology()

        assertTrue(hasQueueTable(dataSourceSecondSchema, queue))
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

}
