package kolbasa.cluster.schema

import kolbasa.cluster.Shard
import kolbasa.producer.ShardId
import kolbasa.schema.Const
import kolbasa.schema.IdSchema
import kolbasa.schema.Node
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaExtractor
import kolbasa.utils.Helpers
import kolbasa.utils.JdbcHelpers.inTransaction
import kolbasa.utils.JdbcHelpers.useStatement
import java.sql.Connection
import java.sql.Statement
import java.util.*
import javax.sql.DataSource

internal object ShardSchema {

    // q__shard
    const val SHARD_TABLE_NAME = Const.INTERNAL_KOLBASA_TABLE_PREFIX + "shard"
    const val SHARD_COLUMN_NAME = "shard"
    const val PRODUCER_NODE_COLUMN_NAME = "producer_node"
    const val CONSUMER_NODE_COLUMN_NAME = "consumer_node"
    const val NEXT_CONSUMER_NODE_COLUMN_NAME = "next_consumer_node"
    const val REVISION_COLUMN_NAME = "revision"

    /**
     * The revision of the first shard map, and of the rows that were there before revisions existed
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

    private const val READ_SHARD_TABLE_STATEMENT = """
        select
            $SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME, $NEXT_CONSUMER_NODE_COLUMN_NAME
        from
            $SHARD_TABLE_NAME
        where
            $SHARD_COLUMN_NAME between ${ShardId.MIN_SHARD} and ${ShardId.MAX_SHARD}
        order by
            $SHARD_COLUMN_NAME
    """

    private val CREATE_SET_REVISION_FUNCTION_STATEMENT = """
        create or replace function $SET_REVISION_FUNCTION_NAME() returns trigger
        language plpgsql set search_path from current as $DOLLAR_QUOTE
        declare
            server_time           timestamptz;
            max_existing_revision timestamptz;
        begin
            if tg_op = 'INSERT' then
                -- An insert with a revision: the first map (0) or repair. Keep it
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

            -- A new revision: the server's time, but always later than every revision in the table,
            -- even when this server's clock is behind. On an empty table max_existing_revision is null, and greatest skips it
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

    // What the catalog has: the table, its revision column (null if there is none), the function's COMMENT, the trigger
    private val READ_SHARD_TABLE_STATE_STATEMENT = """
        select
            to_regclass('$SHARD_TABLE_NAME') is not null as table_exists,
            (
                select attnotnull
                from pg_attribute
                where attrelid = to_regclass('$SHARD_TABLE_NAME') and attname = '$REVISION_COLUMN_NAME' and not attisdropped
            ) as revision_not_null,
            obj_description(to_regprocedure('$SET_REVISION_FUNCTION_NAME()'), 'pg_proc') as function_comment,
            exists (
                select 1
                from pg_trigger
                where tgrelid = to_regclass('$SHARD_TABLE_NAME') and tgname = '$SET_REVISION_TRIGGER_NAME'
            ) as trigger_exists
    """.trimIndent()

    /**
     * Creates `q__shard` with its revision trigger, or brings an existing table up to date: adds the revision column to
     * a table from before revisions, replaces a changed trigger function, restores a missing trigger.
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
        // One transaction: the 'alter table' lock keeps the other clients out until the trigger is there
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
            val revisionNotNull = resultSet.getBoolean("revision_not_null").takeUnless { resultSet.wasNull() }
            val functionComment = resultSet.getString("function_comment")
            val triggerExists = resultSet.getBoolean("trigger_exists")

            if (!tableExists) {
                statements += CREATE_SHARD_TABLE_STATEMENT
            } else {
                // A table from before revisions: add the column and give the existing rows the initial revision
                if (revisionNotNull == null) {
                    statements += "alter table $SHARD_TABLE_NAME add column if not exists $REVISION_COLUMN_NAME timestamptz"
                }
                if (revisionNotNull != true) {
                    statements += "update $SHARD_TABLE_NAME set $REVISION_COLUMN_NAME = '$INITIAL_REVISION' " +
                        "where $REVISION_COLUMN_NAME is null"
                    statements += "alter table $SHARD_TABLE_NAME alter column $REVISION_COLUMN_NAME set not null"
                }
            }

            if (functionComment != SET_REVISION_FUNCTION_COMMENT_PREFIX + SET_REVISION_FUNCTION_HASH) {
                statements += CREATE_SET_REVISION_FUNCTION_STATEMENT
                statements += COMMENT_SET_REVISION_FUNCTION_STATEMENT
            }

            // The trigger goes last. Before the update above, it would turn the initial revisions back into nulls
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
     * Gives every shard that has no row yet a random node of [nodes].
     *
     * With [firstMap] the new rows get [INITIAL_REVISION]: a new cluster has no messages, nothing depends on them yet.
     * Otherwise the trigger gives them the current time.
     */
    fun fillShardTable(dataSource: DataSource, nodes: List<Node>, firstMap: Boolean = false) {
        val shardsPerStatement = 100
        val statements = ShardId.SHARDS_RANGE.chunked(shardsPerStatement).map { shards ->
            val values = shards.map { shard ->
                val randomNode = nodes.random()
                val randomNodeStringId: String = randomNode.id.id
                if (firstMap) {
                    "($shard, '$randomNodeStringId', '$randomNodeStringId', '$INITIAL_REVISION')"
                } else {
                    "($shard, '$randomNodeStringId', '$randomNodeStringId')"
                }
            }

            val columns = if (firstMap) {
                "$SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME, $REVISION_COLUMN_NAME"
            } else {
                "$SHARD_COLUMN_NAME, $PRODUCER_NODE_COLUMN_NAME, $CONSUMER_NODE_COLUMN_NAME"
            }

            """
                insert into $SHARD_TABLE_NAME
                    ($columns)
                values
                    ${values.joinToString(",")}
                on conflict do nothing
            """.trimIndent()
        }

        dataSource.useStatement { statement ->
            statements.forEach { statement.execute(it) }
        }
    }

    /**
     * Whether `q__shard` exists. The catalog answers it, so a failed read is never taken for a missing table
     */
    fun shardTableExists(dataSource: DataSource): Boolean {
        return SchemaExtractor.extractRawSchema(dataSource, setOf(SHARD_TABLE_NAME)).containsKey(SHARD_TABLE_NAME)
    }

    /**
     * Looks for the shard table on [nodes], in node order, and stops at the first one that has every shard.
     *
     * A node whose check or read fails stops the search with an exception that names the node. A failed read is
     * not a missing table.
     */
    fun findShardTable(nodes: SortedMap<Node, DataSource>): ShardTableSearch {
        var firstIncomplete: ShardTableSearch.Incomplete? = null

        for ((node, dataSource) in nodes) {
            val shards = try {
                if (shardTableExists(dataSource))
                    readShards(dataSource)
                else
                    null
            } catch (e: Exception) {
                throw IllegalStateException("Can't read $SHARD_TABLE_NAME on node ${node.id.id}", e)
            }

            if (shards == null) {
                continue
            }

            if (shards.size == ShardId.SHARD_COUNT) {
                return ShardTableSearch.Complete(node, dataSource, shards)
            }

            // A complete table on a later node still wins, so go on
            if (firstIncomplete == null) {
                firstIncomplete = ShardTableSearch.Incomplete(node, dataSource, shards.size)
            }
        }

        // If there isn’t a single incomplete shard table - there is no shard table at all
        return firstIncomplete ?: ShardTableSearch.Missing
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

                    shards[shard] = Shard(
                        shard = shard,
                        producerNode = producerNode,
                        consumerNode = consumerNode,
                        nextConsumerNode = nextConsumerNode
                    )
                }
            }
        }

        return shards
    }

}
