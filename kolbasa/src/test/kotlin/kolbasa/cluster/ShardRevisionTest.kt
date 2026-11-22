package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.cluster.schema.ShardSchema.CONSUMER_NODE_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.PRODUCER_NODE_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.REVISION_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.SHARD_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.SHARD_TABLE_NAME
import kolbasa.producer.ShardId
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.readBoolean
import kolbasa.utils.JdbcHelpers.readInt
import kolbasa.utils.JdbcHelpers.readString
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

class ShardRevisionTest : AbstractPostgreSQLTest() {

    @Test
    fun testNewCluster_RevisionColumnAndTrigger() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        Cluster(dataSources, emptyList()).updateTopology()
        val first = ClusterHelper.readNodes(dataSources).values.first()

        assertRevisionSchema(first)
        assertEquals(ShardId.SHARD_COUNT, first.readInt("select count(*) from $SHARD_TABLE_NAME"))
        assertEquals(0, countRevisionsOtherThanInitial(first))
    }

    @Test
    fun testTrigger_HandEdit_GetsRevisionNewerThanAll() {
        val first = seedShardTable()

        moveShard(first, shard = 5, node = "node-x")
        val revision5 = revisionOf(first, 5)
        assertTrue(revision5 > Instant.EPOCH, "Revision: $revision5")

        moveShard(first, shard = 6, node = "node-x")
        val revision6 = revisionOf(first, 6)
        assertTrue(revision6 > revision5, "Revisions: $revision5, $revision6")

        // The other shards are untouched
        assertEquals(2, countRevisionsOtherThanInitial(first))
    }

    @Test
    fun testTrigger_HandEdit_SmallerOrEqualRevisionIgnored() {
        val first = seedShardTable()
        moveShard(first, shard = 5, node = "node-x")
        val revision5 = revisionOf(first, 5)

        // A smaller revision is replaced
        moveShard(first, shard = 6, node = "node-x", revision = "1960-01-01 00:00:00+00")
        val revision6 = revisionOf(first, 6)
        assertTrue(revision6 > revision5, "Revisions: $revision5, $revision6")

        // The same revision as the row has is replaced too
        moveShard(first, shard = 7, node = "node-x", revision = ShardSchema.INITIAL_REVISION)
        val revision7 = revisionOf(first, 7)
        assertTrue(revision7 > revision6, "Revisions: $revision6, $revision7")
    }

    @Test
    fun testTrigger_NewerRevisionKept() {
        val first = seedShardTable()

        moveShard(first, shard = 5, node = "node-x", revision = "2100-01-01 00:00:00+00")

        assertEquals(Instant.parse("2100-01-01T00:00:00Z"), revisionOf(first, 5))
    }

    @Test
    fun testTrigger_EmptyUpdate_RevisionStays() {
        val first = seedShardTable()
        moveShard(first, shard = 5, node = "node-x")
        val revision5 = revisionOf(first, 5)

        first.useStatement { statement ->
            // The same node again
            statement.execute(
                "update $SHARD_TABLE_NAME set $PRODUCER_NODE_COLUMN_NAME = 'node-x', $CONSUMER_NODE_COLUMN_NAME = 'node-x' " +
                    "where $SHARD_COLUMN_NAME = 5"
            )
            // Only a smaller revision, the content is the same
            statement.execute(
                "update $SHARD_TABLE_NAME set $REVISION_COLUMN_NAME = '${ShardSchema.INITIAL_REVISION}' " +
                    "where $SHARD_COLUMN_NAME = 5"
            )
        }

        assertEquals(revision5, revisionOf(first, 5))
    }

    @Test
    fun testTrigger_HandEditFromAnotherSchema() {
        // The map lives in the 'first' schema, the edit comes from a session that works in 'public'
        seedShardTable(dataSourceFirstSchema)

        dataSource.useStatement { statement ->
            statement.execute(
                "update $FIRST_SCHEMA_NAME.$SHARD_TABLE_NAME " +
                    "set $PRODUCER_NODE_COLUMN_NAME = 'node-x', $CONSUMER_NODE_COLUMN_NAME = 'node-x' " +
                    "where $SHARD_COLUMN_NAME = 5"
            )
        }

        assertTrue(revisionOf(dataSourceFirstSchema, 5) > Instant.EPOCH)
    }

    @Test
    fun testCreateOrUpdateShardTable_ChangedAtTheSameMoment_SecondTryFinishes() {
        val first = seedShardTable()
        first.useStatement { statement ->
            statement.execute("drop trigger ${ShardSchema.SET_REVISION_FUNCTION_NAME} on $SHARD_TABLE_NAME")
        }

        // Another client is restoring the trigger right now and hasn't committed yet
        first.connection.use { otherClient ->
            otherClient.autoCommit = false
            ShardSchema.createOrUpdateShardTable(otherClient)

            val update = CompletableFuture.runAsync { ShardSchema.createOrUpdateShardTable(first) }

            // Our update waits for the other one, then loses on 'create trigger' once it commits
            waitUntil { dataSource.readInt(WAITING_TRIGGER_CREATES) > 0 }
            otherClient.commit()

            update.get(30, TimeUnit.SECONDS)
        }

        assertRevisionSchema(first)
        assertEquals(0, countRevisionsOtherThanInitial(first))
    }

    @Test
    fun testCreateOrUpdateShardTable_ChangedFunctionReplaced() {
        val first = seedShardTable()

        // A function from another kolbasa version: it does nothing
        first.useStatement { statement ->
            statement.execute(
                """
                create or replace function ${ShardSchema.SET_REVISION_FUNCTION_NAME}() returns trigger
                language plpgsql as ${'$'}${'$'} begin return new; end ${'$'}${'$'}
                """.trimIndent()
            )
            statement.execute("comment on function ${ShardSchema.SET_REVISION_FUNCTION_NAME}() is 'kolbasa-shard-revision:old'")
        }

        ShardSchema.createOrUpdateShardTable(first)

        assertRevisionSchema(first)
        moveShard(first, shard = 5, node = "node-x")
        assertTrue(revisionOf(first, 5) > Instant.EPOCH)
    }

    // A new shard table on the node with the smallest id, seeded: every row has the initial revision
    private fun seedShardTable(): DataSource {
        val nodes = ClusterHelper.readNodes(listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema))
        val first = nodes.values.first()
        seedShardTable(first, nodes.keys.toList())
        return first
    }

    private fun seedShardTable(dataSource: DataSource) {
        val nodes = ClusterHelper.readNodes(listOf(dataSource))
        seedShardTable(dataSource, nodes.keys.toList())
    }

    private fun seedShardTable(dataSource: DataSource, nodes: List<kolbasa.schema.Node>) {
        ShardSchema.createOrUpdateShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes)
    }

    // Gives the shard to another node by hand, with a revision of its own if one is given
    private fun moveShard(dataSource: DataSource, shard: Int, node: String, revision: String? = null) {
        val setRevision = if (revision != null) ", $REVISION_COLUMN_NAME = '$revision'" else ""
        dataSource.useStatement { statement ->
            statement.execute(
                "update $SHARD_TABLE_NAME " +
                    "set $PRODUCER_NODE_COLUMN_NAME = '$node', $CONSUMER_NODE_COLUMN_NAME = '$node'$setRevision " +
                    "where $SHARD_COLUMN_NAME = $shard"
            )
        }
    }

    private fun revisionOf(dataSource: DataSource, shard: Int): Instant {
        val sql = "select $REVISION_COLUMN_NAME from $SHARD_TABLE_NAME where $SHARD_COLUMN_NAME = $shard"
        return dataSource.useStatement(sql) { resultSet ->
            check(resultSet.next()) { "No shard $shard" }
            resultSet.getTimestamp(1).toInstant()
        }
    }

    private fun countRevisionsOtherThanInitial(dataSource: DataSource): Int {
        return dataSource.readInt(
            "select count(*) from $SHARD_TABLE_NAME where $REVISION_COLUMN_NAME <> '${ShardSchema.INITIAL_REVISION}'"
        )
    }

    // The revision column is 'not null' without a default, the trigger is there, the function is the current one
    private fun assertRevisionSchema(dataSource: DataSource) {
        val column = """
            from information_schema.columns
            where table_schema = current_schema() and table_name = '$SHARD_TABLE_NAME' and column_name = '$REVISION_COLUMN_NAME'
        """.trimIndent()
        assertEquals("NO", dataSource.readString("select is_nullable $column"))
        assertTrue(dataSource.readBoolean("select column_default is null $column"))

        assertTrue(
            dataSource.readBoolean(
                """
                select exists (
                    select 1 from pg_trigger
                    where tgrelid = to_regclass('$SHARD_TABLE_NAME') and tgname = '${ShardSchema.SET_REVISION_FUNCTION_NAME}'
                )
                """.trimIndent()
            )
        )
        assertEquals(
            "kolbasa-shard-revision:${ShardSchema.SET_REVISION_FUNCTION_HASH}",
            dataSource.readString(
                "select obj_description(to_regprocedure('${ShardSchema.SET_REVISION_FUNCTION_NAME}()'), 'pg_proc')"
            )
        )
    }

    private fun waitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
        while (!condition()) {
            check(System.nanoTime() < deadline) { "The condition wasn't met in 30 seconds" }
            Thread.sleep(20)
        }
    }

    private companion object {
        // Creates of the revision trigger in this database that wait for a lock
        val WAITING_TRIGGER_CREATES = """
            select count(*)
            from pg_stat_activity
            where
                datname = current_database() and
                wait_event_type = 'Lock' and
                position('create trigger ${ShardSchema.SET_REVISION_FUNCTION_NAME}' in query) > 0
        """.trimIndent()
    }

}
