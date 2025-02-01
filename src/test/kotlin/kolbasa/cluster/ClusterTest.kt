package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import kolbasa.cluster.schema.ShardSchema
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.schema.IdSchema
import java.sql.Statement
import javax.sql.DataSource
import kotlin.test.*

class ClusterTest : AbstractPostgresqlTest() {

    @Test
    fun testInitCluster_If_No_DataSources() {
        val cluster = Cluster(emptyList())

        assertFailsWith<IllegalStateException> {
            cluster.initAndScheduleStateUpdate()
        }
    }

    @Test
    fun testInitCluster_If_State_Not_Initialized() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val cluster = Cluster(dataSources)

        assertFailsWith<IllegalStateException> {
            cluster.getState()
        }
    }

    @Test
    fun testInitCluster_If_State_The_Same() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)

        val cluster = Cluster(dataSources)

        // First initialization
        cluster.updateStateOnce()
        val firstState = cluster.getState()

        // Second initialization, check that states are the same
        cluster.updateStateOnce()
        val secondState = cluster.getState()

        assertSame(firstState, secondState)
        assertTrue(firstState.nodes.isNotEmpty(), "State: $firstState")
    }

    @Test
    fun testInitCluster_If_State_Not_The_Same() {
        val dataSources = mapOf(
            "public" to dataSource,
            FIRST_SCHEMA_NAME to dataSourceFirstSchema,
            SECOND_SCHEMA_NAME to dataSourceSecondSchema
        )

        val cluster = Cluster(dataSources.values.toList())

        // First shard table initialization/read
        cluster.updateStateOnce()
        val firstState = cluster.getState()

        // ---------------------------------------------------------------------------------------
        // Make shard changes
        val shardToChange = Shard.randomShard()
        val currentProducerConsumerName = requireNotNull(firstState.shards[shardToChange]?.producerNode)
        val newProducerConsumerName = (firstState.nodes.keys - currentProducerConsumerName).random()
        assertNotEquals(currentProducerConsumerName, newProducerConsumerName)

        val shardTables = findShardTables(dataSources).filter { foundShardTable ->
            foundShardTable.numberOfTables > 0
        }
        assertEquals(1, shardTables.size)
        shardTables.first().datasource.useStatement { statement: Statement ->
            val sql = """
                update
                    ${ShardSchema.SHARD_TABLE_NAME}
                set
                    ${ShardSchema.PRODUCER_NODE_COLUMN_NAME} = '$newProducerConsumerName',
                    ${ShardSchema.CONSUMER_NODE_COLUMN_NAME} = '$newProducerConsumerName'
                where
                    ${ShardSchema.SHARD_COLUMN_NAME} = $shardToChange
            """.trimIndent()
            statement.execute(sql)
        }
        // ---------------------------------------------------------------------------------------

        // Second shard table read
        cluster.updateStateOnce()
        val secondState = cluster.getState()

        assertNotEquals(firstState, secondState)
        assertEquals(secondState.shards[shardToChange]?.producerNode, newProducerConsumerName)
        assertEquals(secondState.shards[shardToChange]?.consumerNode, newProducerConsumerName)
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
        cluster.updateStateOnce()
        val firstShardIDs = dataSources.mapNotNull { (_, dataSource) ->
            IdSchema.readNodeInfo(dataSource)
        }
        assertEquals(dataSources.size, firstShardIDs.size, "Data sources: $dataSources, shardIds: $firstShardIDs")

        // Second initialization, check that IDs are the same
        cluster.updateStateOnce()
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
        cluster.updateStateOnce()

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
        assertEquals(smallestExistingId.serverId, nodeIdWithShardTable)
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
                FoundShardTable(numberOfTables, thisNodeId.serverId, schema, dataSource)
            }
    }

    private data class FoundShardTable(
        val numberOfTables: Int,
        val nodeId: String,
        val schema: String,
        val datasource: DataSource
    )

}
