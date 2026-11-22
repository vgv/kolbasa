package kolbasa.cluster

import kolbasa.cluster.schema.ShardSchema
import kolbasa.cluster.schema.ShardSchema.CONSUMER_NODE_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.NEXT_CONSUMER_NODE_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.PRODUCER_NODE_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.REVISION_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.SHARD_COLUMN_NAME
import kolbasa.cluster.schema.ShardSchema.SHARD_TABLE_NAME
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ShardFingerprintTest : AbstractPostgreSQLTest() {

    @Test
    fun testFingerprint_SqlEqualsKotlin() {
        val nodes = ClusterHelper.readNodes(listOf(dataSource))
        ShardSchema.createOrUpdateShardTable(dataSource)
        ShardSchema.fillShardTable(dataSource, nodes.keys.toList(), initialRevision = true)

        // A new table: every row has the initial revision
        val seeded = assertFingerprintsEqual()

        // Hand edits: the trigger sets the current time, an explicit revision has all six digits of microseconds
        execute(
            "update $SHARD_TABLE_NAME set $PRODUCER_NODE_COLUMN_NAME = 'node-x', $CONSUMER_NODE_COLUMN_NAME = 'node-x' " +
                "where $SHARD_COLUMN_NAME = 5"
        )
        execute(
            "update $SHARD_TABLE_NAME set $PRODUCER_NODE_COLUMN_NAME = 'node-x', $CONSUMER_NODE_COLUMN_NAME = 'node-x', " +
                "$REVISION_COLUMN_NAME = '2099-12-31 23:59:59.999999+00' where $SHARD_COLUMN_NAME = 6"
        )
        val handEdits = assertFingerprintsEqual()

        // A migrating shard: its consumer is null
        execute(
            "update $SHARD_TABLE_NAME set $PRODUCER_NODE_COLUMN_NAME = 'node-y', $CONSUMER_NODE_COLUMN_NAME = null, " +
                "$NEXT_CONSUMER_NODE_COLUMN_NAME = 'node-y' where $SHARD_COLUMN_NAME = 7"
        )
        val migrating = assertFingerprintsEqual()

        // A shard out of range: both skip the row, so the md5 stays the same
        execute(
            "insert into $SHARD_TABLE_NAME($SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME) " +
                "values (5000, 'node-z', 'node-z')"
        )
        val outOfRange = assertFingerprintsEqual()

        assertEquals(3, setOf(seeded, handEdits, migrating).size)
        assertEquals(migrating, outOfRange)
    }

    // The md5 from the node is the md5 that Kotlin computes from the rows it reads
    private fun assertFingerprintsEqual(): String {
        val fingerprint = ShardSchema.readFingerprint(dataSource)
        assertEquals(ShardSchema.fingerprint(ShardSchema.readShards(dataSource).values), fingerprint)
        return fingerprint
    }

    private fun execute(sql: String) {
        dataSource.useStatement { statement -> statement.execute(sql) }
    }
}
