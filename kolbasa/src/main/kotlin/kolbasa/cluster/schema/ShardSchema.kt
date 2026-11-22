package kolbasa.cluster.schema

import kolbasa.cluster.Shard
import kolbasa.producer.ShardId
import kolbasa.schema.Const
import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.utils.Helpers
import kolbasa.utils.JdbcHelpers.inTransaction
import kolbasa.utils.JdbcHelpers.readString
import kolbasa.utils.JdbcHelpers.usePreparedStatement
import kolbasa.utils.JdbcHelpers.useStatement
import java.sql.Connection
import java.sql.Statement
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.*
import javax.sql.DataSource

internal object ShardSchema {

    // q__shard
    const val SHARD_TABLE_NAME = Const.INTERNAL_KOLBASA_TABLE_PREFIX + "shard"
    const val SHARD_COLUMN_NAME = "shard"
    const val PRODUCER_NODE_COLUMN_NAME = "producer_node"
    const val CONSUMER_NODE_COLUMN_NAME = "consumer_node"
    const val NEXT_CONSUMER_NODE_COLUMN_NAME = "next_consumer_node"

    /**
     * The revision of a row: when its content last changed. It is a simple kind of hybrid logical clock, set by the
     * [SET_REVISION_FUNCTION_NAME] trigger
     */
    const val REVISION_COLUMN_NAME = "revision"

    /**
     * The revision of the rows kolbasa adds itself, with a random node: in a new cluster, or for a shard that no node
     * has. It is less than any real revision, so such a row loses to any row that was ever changed
     */
    const val INITIAL_REVISION = "1970-01-01 00:00:00+00"

    // The trigger that sets the revision, and its function, have the same name
    const val SET_REVISION_FUNCTION_NAME = SHARD_TABLE_NAME + "_set_revision"
    private const val SET_REVISION_TRIGGER_NAME = SET_REVISION_FUNCTION_NAME

    // The COMMENT on the function: 'kolbasa-shard-revision:<md5>', the same way as for the put functions
    private const val SET_REVISION_FUNCTION_COMMENT_PREFIX = "kolbasa-shard-revision:"

    private const val DOLLAR_QUOTE = "\$kolbasa\$"

    const val CREATE_SHARD_TABLE_STATEMENT = """
        create table if not exists $SHARD_TABLE_NAME(
            $SHARD_COLUMN_NAME int not null primary key,
            $PRODUCER_NODE_COLUMN_NAME varchar(${IdSchema.ID_COLUMN_LENGTH}) not null,
            $CONSUMER_NODE_COLUMN_NAME varchar(${IdSchema.ID_COLUMN_LENGTH}),
            $NEXT_CONSUMER_NODE_COLUMN_NAME varchar(${IdSchema.ID_COLUMN_LENGTH}),
            $REVISION_COLUMN_NAME timestamptz not null,
            check (
                ($PRODUCER_NODE_COLUMN_NAME=$CONSUMER_NODE_COLUMN_NAME and $NEXT_CONSUMER_NODE_COLUMN_NAME is null) or
                ($PRODUCER_NODE_COLUMN_NAME=$NEXT_CONSUMER_NODE_COLUMN_NAME and $CONSUMER_NODE_COLUMN_NAME is null)
            )
        )
    """

    val READ_SHARD_TABLE_STATEMENT = """
        select
            $SHARD_COLUMN_NAME,
            $PRODUCER_NODE_COLUMN_NAME,
            $CONSUMER_NODE_COLUMN_NAME,
            $NEXT_CONSUMER_NODE_COLUMN_NAME,
            $REVISION_COLUMN_NAME
        from
            $SHARD_TABLE_NAME
        where
            $SHARD_COLUMN_NAME between ${ShardId.MIN_SHARD} and ${ShardId.MAX_SHARD}
        order by
            $SHARD_COLUMN_NAME
    """.trimIndent()

    // The md5 of the rows that READ_SHARD_TABLE_STATEMENT reads: one line per shard, in shard order, the revision in
    // microseconds. fingerprint() builds the same text in Kotlin, change both together
    private val READ_SHARD_TABLE_FINGERPRINT_STATEMENT = """
        select
            md5(coalesce(string_agg(
                $SHARD_COLUMN_NAME || ',' ||
                $PRODUCER_NODE_COLUMN_NAME || ',' ||
                coalesce($CONSUMER_NODE_COLUMN_NAME, '') || ',' ||
                coalesce($NEXT_CONSUMER_NODE_COLUMN_NAME, '') || ',' ||
                (extract(epoch from $REVISION_COLUMN_NAME) * 1000000)::bigint,
                E'\n' order by $SHARD_COLUMN_NAME
            ), ''))
        from
            $SHARD_TABLE_NAME
        where
            $SHARD_COLUMN_NAME between ${ShardId.MIN_SHARD} and ${ShardId.MAX_SHARD}
    """.trimIndent()

    private val CREATE_SET_REVISION_FUNCTION_STATEMENT = """
        create or replace function $SET_REVISION_FUNCTION_NAME() returns trigger
        language plpgsql set search_path from current as $DOLLAR_QUOTE
        declare
            server_time           timestamptz;
            max_existing_revision timestamptz;
        begin
            if tg_op = 'INSERT' then
                -- An insert with a revision: the rows seeded into a new table (0) or repair. Keep it
                if new.$REVISION_COLUMN_NAME is not null then
                    return new;
                end if;

                -- An insert without a revision: a hand edit or an old client. A new revision is set below

            elsif tg_op = 'UPDATE' then
                -- The statement brings a newer revision of its own: keep it
                if new.$REVISION_COLUMN_NAME > old.$REVISION_COLUMN_NAME then
                    return new;
                end if;

                -- Nothing in the row has changed, so its revision stays the same.
                -- This check goes second: repair may write the same row with a newer revision, and that one must stay
                if (new.$PRODUCER_NODE_COLUMN_NAME, new.$CONSUMER_NODE_COLUMN_NAME, new.$NEXT_CONSUMER_NODE_COLUMN_NAME)
                   is not distinct from
                   (old.$PRODUCER_NODE_COLUMN_NAME, old.$CONSUMER_NODE_COLUMN_NAME, old.$NEXT_CONSUMER_NODE_COLUMN_NAME) then
                    new.$REVISION_COLUMN_NAME := old.$REVISION_COLUMN_NAME;
                    return new;
                end if;

                -- A real change without a newer revision: a hand edit or an old butcher.
                -- A smaller or equal revision set by the statement is ignored, a new revision is set below
            end if;

            -- A new revision, a simple hybrid logical clock: the server's time, but always later than every revision
            -- in the table, even when this server's clock is behind. On an empty table max_existing_revision is null,
            -- and greatest skips it
            server_time := clock_timestamp();
            select max($REVISION_COLUMN_NAME) into max_existing_revision from $SHARD_TABLE_NAME;
            new.$REVISION_COLUMN_NAME := greatest(server_time, max_existing_revision + interval '1 microsecond');
            return new;
        end
        $DOLLAR_QUOTE
    """.trimIndent()

    /**
     * The md5 of the function's create statement. It is stored in the function's COMMENT, so a changed function
     * is noticed and replaced
     */
    val SET_REVISION_FUNCTION_HASH = Helpers.md5Hash(CREATE_SET_REVISION_FUNCTION_STATEMENT)

    private val COMMENT_SET_REVISION_FUNCTION_STATEMENT =
        "comment on function $SET_REVISION_FUNCTION_NAME() is '$SET_REVISION_FUNCTION_COMMENT_PREFIX$SET_REVISION_FUNCTION_HASH'"

    // PostgreSQL 10 has no 'execute function' yet, only 'execute procedure'
    private val CREATE_SET_REVISION_TRIGGER_STATEMENT = """
        create trigger $SET_REVISION_TRIGGER_NAME
        before insert or update on $SHARD_TABLE_NAME
        for each row execute procedure $SET_REVISION_FUNCTION_NAME()
    """.trimIndent()

    // What the catalog has: the table, the function's COMMENT, the trigger
    private val READ_SHARD_TABLE_STATE_STATEMENT = """
        select
            to_regclass('$SHARD_TABLE_NAME') is not null as table_exists,
            obj_description(to_regprocedure('$SET_REVISION_FUNCTION_NAME()'), 'pg_proc') as function_comment,
            exists (
                select 1
                from pg_trigger
                where tgrelid = to_regclass('$SHARD_TABLE_NAME') and tgname = '$SET_REVISION_TRIGGER_NAME'
            ) as trigger_exists
    """.trimIndent()

    /**
     * Creates `q__shard` with its revision trigger, or brings an existing table up to date: replaces a changed trigger
     * function, restores a missing trigger.
     *
     * It checks the catalog first and runs only what is missing, so on an up-to-date table it is one query. All the
     * changes run in one transaction.
     */
    fun createOrUpdateShardTable(dataSource: DataSource) {
        try {
            createOrUpdateShardTableOnce(dataSource)
        } catch (_: Exception) {
            // Another client changed the table at the same moment, and our change lost: on the catalog, for example,
            // when both create the table. Its changes are committed by now, so the second try finds them and does only
            // what is still missing
            createOrUpdateShardTableOnce(dataSource)
        }
    }

    private fun createOrUpdateShardTableOnce(dataSource: DataSource) {
        // One transaction: no other client sees a new table without its trigger
        dataSource.inTransaction { connection ->
            createOrUpdateShardTable(connection)
        }
    }

    /**
     * The same as [createOrUpdateShardTable], but in the caller's transaction, and without a second try
     */
    fun createOrUpdateShardTable(connection: Connection) {
        val statements = mutableListOf<String>()

        connection.useStatement(READ_SHARD_TABLE_STATE_STATEMENT) { resultSet ->
            resultSet.next()
            val tableExists = resultSet.getBoolean("table_exists")
            val functionComment = resultSet.getString("function_comment")
            val triggerExists = resultSet.getBoolean("trigger_exists")

            if (!tableExists) {
                statements += CREATE_SHARD_TABLE_STATEMENT
            }

            if (functionComment != SET_REVISION_FUNCTION_COMMENT_PREFIX + SET_REVISION_FUNCTION_HASH) {
                statements += CREATE_SET_REVISION_FUNCTION_STATEMENT
                statements += COMMENT_SET_REVISION_FUNCTION_STATEMENT
            }

            // The trigger goes last: it needs the table and the function
            if (!triggerExists) {
                statements += CREATE_SET_REVISION_TRIGGER_STATEMENT
            }
        }

        // An up-to-date table: nothing to change
        if (statements.isEmpty()) {
            return
        }

        connection.useStatement { statement ->
            statements.forEach { statement.execute(it) }
        }
    }

    /**
     * Gives each of [shardsToFill] that has no row yet a random node of [nodes].
     *
     * The new rows get [INITIAL_REVISION]: a random node is a guess, and a guess must lose to any real row, wherever
     * that row turns up later.
     */
    fun fillShardTable(dataSource: DataSource, nodes: List<Node>, shardsToFill: Iterable<Int> = ShardId.SHARDS_RANGE) {
        val shardsPerStatement = 100
        val statements = shardsToFill.chunked(shardsPerStatement).map { chunk ->
            val values = chunk.map { shard ->
                val randomNode = nodes.random()
                val randomNodeStringId: String = randomNode.id.id
                "($shard, '$randomNodeStringId', '$randomNodeStringId', '$INITIAL_REVISION')"
            }

            """
                insert into $SHARD_TABLE_NAME
                    ($SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME, $REVISION_COLUMN_NAME)
                values
                    ${values.joinToString(",")}
                on conflict do nothing
            """.trimIndent()
        }

        dataSource.useStatement { statement ->
            statements.forEach { statement.execute(it) }
        }
    }

    fun readShards(dataSource: DataSource): Map<ShardId, Shard> {
        val shards = hashMapOf<ShardId, Shard>()

        dataSource.useStatement { statement: Statement ->
            statement.executeQuery(READ_SHARD_TABLE_STATEMENT).use { resultSet ->
                while (resultSet.next()) {
                    val shard = ShardId.of(resultSet.getInt(SHARD_COLUMN_NAME))
                    val producerNode = NodeId(resultSet.getString(PRODUCER_NODE_COLUMN_NAME))
                    val consumerNode = resultSet.getString(CONSUMER_NODE_COLUMN_NAME)?.let { NodeId(it) }
                    val nextConsumerNode = resultSet.getString(NEXT_CONSUMER_NODE_COLUMN_NAME)?.let { NodeId(it) }
                    val revision = resultSet.getObject(REVISION_COLUMN_NAME, OffsetDateTime::class.java).toInstant()

                    shards[shard] = Shard(
                        shard = shard,
                        producerNode = producerNode,
                        consumerNode = consumerNode,
                        nextConsumerNode = nextConsumerNode,
                        revision = revision
                    )
                }
            }
        }

        return shards
    }

    /**
     * The revision for a change of the shard map that kolbasa makes itself: a simple kind of hybrid logical clock, the
     * same as in the trigger. It is the current time, or the newest revision of [shards] plus one microsecond,
     * whichever is later. In whole microseconds, as PostgreSQL keeps it
     */
    fun nextRevision(shards: Collection<Shard>, now: Instant = Instant.now()): Instant {
        val currentTime = now.truncatedTo(ChronoUnit.MICROS)
        val newestRevision = shards.maxOfOrNull { it.revision } ?: return currentTime

        return maxOf(currentTime, newestRevision.plus(1, ChronoUnit.MICROS))
    }

    /**
     * Writes [shards] to the shard table of a node, each row only if the table has no row of that shard, or an older
     * one. Many clients may write the same rows to the same node at the same moment, the result is the same.
     *
     * The trigger keeps the revisions written here: an insert brings its own revision, an update brings a newer one
     */
    fun writeShards(dataSource: DataSource, shards: Collection<Shard>) {
        // In shard order, so that two clients writing the same rows lock them in the same order, without a deadlock
        val sortedShards = shards.sortedBy { it.shard }
        val values = sortedShards.joinToString(", ") { "(?, ?, ?, ?, ?)" }

        val sql = """
            insert into $SHARD_TABLE_NAME
                ($SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME, $NEXT_CONSUMER_NODE_COLUMN_NAME,
                $REVISION_COLUMN_NAME)
            values
                $values
            on conflict ($SHARD_COLUMN_NAME) do update set
                $PRODUCER_NODE_COLUMN_NAME = excluded.$PRODUCER_NODE_COLUMN_NAME,
                $CONSUMER_NODE_COLUMN_NAME = excluded.$CONSUMER_NODE_COLUMN_NAME,
                $NEXT_CONSUMER_NODE_COLUMN_NAME = excluded.$NEXT_CONSUMER_NODE_COLUMN_NAME,
                $REVISION_COLUMN_NAME = excluded.$REVISION_COLUMN_NAME
            where
                $SHARD_TABLE_NAME.$REVISION_COLUMN_NAME < excluded.$REVISION_COLUMN_NAME
        """.trimIndent()

        dataSource.usePreparedStatement(sql) { preparedStatement ->
            var index = 1
            for (shard in sortedShards) {
                preparedStatement.setInt(index++, shard.shard.id)
                preparedStatement.setString(index++, shard.producerNode.id)
                preparedStatement.setString(index++, shard.consumerNode?.id)
                preparedStatement.setString(index++, shard.nextConsumerNode?.id)
                preparedStatement.setObject(index++, OffsetDateTime.ofInstant(shard.revision, ZoneOffset.UTC))
            }
            preparedStatement.executeUpdate()
        }
    }

    /**
     * One map out of the copies on the nodes: for every shard, the row with the newest revision. If several copies have
     * that revision, the row of the node with the smallest id
     */
    fun mergeShards(copies: SortedMap<Node, Map<ShardId, Shard>>): Map<ShardId, Shard> {
        val merged = hashMapOf<ShardId, Shard>()

        // In node order, so the row of the node with the smallest id stays if the revisions are equal
        for (copy in copies.values) {
            for ((shardId, shard) in copy) {
                val current = merged[shardId]
                if (current == null || shard.revision > current.revision) {
                    merged[shardId] = shard
                }
            }
        }

        return merged
    }

    /**
     * The md5 of the shard table, computed by the node. Equal to [fingerprint] of the rows that [readShards] returns,
     * so a client can tell whether its map is still up to date without reading the rows
     */
    fun readFingerprint(dataSource: DataSource): String {
        return dataSource.readString(READ_SHARD_TABLE_FINGERPRINT_STATEMENT)
    }

    /**
     * The md5 that READ_SHARD_TABLE_FINGERPRINT_STATEMENT computes on a node that holds these rows.
     * Change both together
     */
    fun fingerprint(shards: Collection<Shard>): String {
        val lines = mutableListOf<String>()

        shards.sortedBy { it.shard.id }.forEach { shard ->
            val shardId = shard.shard.id
            val producerNode = shard.producerNode.id
            val consumerNode = shard.consumerNode?.id ?: ""
            val nextConsumerNode = shard.nextConsumerNode?.id ?: ""
            val revision = ChronoUnit.MICROS.between(Instant.EPOCH, shard.revision)

            lines += "$shardId,$producerNode,$consumerNode,$nextConsumerNode,$revision"
        }

        return Helpers.md5Hash(lines.joinToString("\n"))
    }

}
