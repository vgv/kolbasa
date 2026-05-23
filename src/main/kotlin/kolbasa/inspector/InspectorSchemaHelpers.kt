package kolbasa.inspector

import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.schema.Const
import kolbasa.utils.JdbcHelpers.readLong
import java.sql.Connection

internal object InspectorSchemaHelpers {

    private const val TOUCHED = "${Const.PROCESSING_AT_COLUMN_NAME} is not null"
    private const val UNTOUCHED = "${Const.PROCESSING_AT_COLUMN_NAME} is null"

    private const val VISIBLE = "${Const.SCHEDULED_AT_COLUMN_NAME} <= statement_timestamp()"
    private const val NOT_VISIBLE = "${Const.SCHEDULED_AT_COLUMN_NAME} > statement_timestamp()"

    private const val EXHAUSTED = "${Const.REMAINING_ATTEMPTS_COLUMN_NAME} <= 0"
    private const val NOT_EXHAUSTED = "${Const.REMAINING_ATTEMPTS_COLUMN_NAME} > 0"

    // Message state SQL conditions
    private const val SCHEDULED_CONDITION = "$UNTOUCHED and $NOT_VISIBLE and $NOT_EXHAUSTED "
    private const val READY_CONDITION = "$UNTOUCHED and $VISIBLE and $NOT_EXHAUSTED"
    private const val IN_FLIGHT_CONDITION = "$TOUCHED and $NOT_VISIBLE"
    private const val RETRY_CONDITION = "$TOUCHED and $VISIBLE and $NOT_EXHAUSTED"
    private const val DEAD_CONDITION = "$VISIBLE and $EXHAUSTED"

    fun generateCountWithFilterQuery(connection: Connection, queue: Queue<*>, options: CountOptions): QueryAndSample {
        val samplePercent = effectiveSamplePercent(connection, queue, options.samplePercent)

        var query = """
            select
                count(*) filter (where $SCHEDULED_CONDITION),
                count(*) filter (where $READY_CONDITION),
                count(*) filter (where $IN_FLIGHT_CONDITION),
                count(*) filter (where $RETRY_CONDITION),
                count(*) filter (where $DEAD_CONDITION)
            from ${queue.dbTableName} tablesample system ($samplePercent)"""

        if (options.filter != null) {
            query += " where ${options.filter.toSqlClause()}"
        }

        return QueryAndSample(query, samplePercent)
    }

    fun <V> generateDistinctValuesQuery(
        connection: Connection,
        queue: Queue<*>,
        metaField: MetaField<V>,
        limit: Int,
        options: DistinctValuesOptions
    ): QueryAndSample {
        val samplePercent = effectiveSamplePercent(connection, queue, options.samplePercent)

        var query = "select ${metaField.dbColumnName}, count(*) from ${queue.dbTableName} tablesample system ($samplePercent)"
        if (options.filter != null) {
            query += " where ${options.filter.toSqlClause()}"
        }
        query += " group by ${metaField.dbColumnName}"
        if (options.order != null) {
            query += " order by count(*) ${options.order.sql}"
        }
        query += " limit $limit"

        return QueryAndSample(query, samplePercent)
    }

    fun generateSizeQuery(queue: Queue<*>): String {
        return "select pg_total_relation_size('${queue.dbTableName}')"
    }

    fun generateIsEmptyQuery(queue: Queue<*>): String {
        return "select not exists (select 1 from ${queue.dbTableName} limit 1)"
    }

    fun generateIsDeadOrEmptyQuery(queue: Queue<*>): String {
        return "select not exists (select 1 from ${queue.dbTableName} where not ($DEAD_CONDITION) limit 1)"
    }

    fun generateMessageAgeQuery(queue: Queue<*>): String {
        return """
            select
                (select extract(epoch from (statement_timestamp() - ${Const.CREATED_AT_COLUMN_NAME}))
                 from ${queue.dbTableName}
                 order by ${Const.ID_COLUMN_NAME} asc
                 limit 1) as oldest_message_age,
                (select extract(epoch from (statement_timestamp() - ${Const.CREATED_AT_COLUMN_NAME}))
                 from ${queue.dbTableName}
                 order by ${Const.ID_COLUMN_NAME} desc
                 limit 1) as newest_message_age,
                (select extract(epoch from (statement_timestamp() - ${Const.SCHEDULED_AT_COLUMN_NAME}))
                 from ${queue.dbTableName}
                 where $VISIBLE and $NOT_EXHAUSTED
                 order by ${Const.SCHEDULED_AT_COLUMN_NAME} asc
                 limit 1) as oldest_ready_or_retry_message_age
                 """.trimIndent()
    }

    private fun effectiveSamplePercent(
        connection: Connection,
        queue: Queue<*>,
        sample: Float
    ): Float {
        if (sample != CountOptions.YOU_KNOW_BETTER) {
            return sample
        }

        val sql =
            "select rel_size/current_setting('block_size')::int from pg_relation_size('${queue.dbTableName}') as rel_size"
        val relationPages = connection.readLong(sql)
        return calculateSamplePercent(relationPages)
    }

    internal fun calculateSamplePercent(relationPages: Long): Float {
        if (relationPages == 0L) {
            // Table is empty
            return READ_ALL_SAMPLE_PERCENT
        }

        // how many pages we need to scan to get a sample of DEFAULT_SAMPLE_PERCENT?
        var pagesToScan = (relationPages * (DEFAULT_SAMPLE_PERCENT / 100)).toLong()
        if (pagesToScan < MIN_PAGES_SCAN) {
            pagesToScan = MIN_PAGES_SCAN
        } else if (pagesToScan > MAX_PAGES_SCAN) {
            pagesToScan = MAX_PAGES_SCAN
        }

        var samplePercent = 100.0f * pagesToScan / relationPages
        if (samplePercent > READ_ALL_SAMPLE_PERCENT) {
            samplePercent = READ_ALL_SAMPLE_PERCENT
        }

        return samplePercent
    }

    internal const val DEFAULT_SAMPLE_PERCENT: Float = 1.0f
    internal const val READ_ALL_SAMPLE_PERCENT: Float = 100.0f

    // Absolute minimum number of pages to sample to get somewhat accurate estimates.
    // It doesn't matter how big or small the table is, we will scan at least this many pages
    internal const val MIN_PAGES_SCAN: Long = 100

    // Absolute maximum number of pages to sample to get results in a reasonable time even for very large tables.
    // It doesn't matter how big or small the table is, we will scan at most this many pages
    internal const val MAX_PAGES_SCAN: Long = 1000
}

internal data class QueryAndSample(
    val sql: String,
    val samplePercent: Float
)
