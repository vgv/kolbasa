package kolbasa.stats.task

import kolbasa.schema.Const
import kolbasa.stats.Measure
import kolbasa.stats.QueueDump
import kotlin.math.min

internal object StatsSchemaHelpers {

    private const val STATS_TABLE_NAME = "q__stats"
    private const val STATS_INTERNAL_TABLE_NAME = "q__stats_internal"
    private const val QUEUE_NAME_COLUMN_NAME = "queue"
    private const val LAST_UPDATE_COLUMN_NAME = "last_update"
    private const val MEASURE_NAME_COLUMN_NAME = "measure"
    private const val TICK_COLUMN_NAME = "tick"
    private const val VALUE_COLUMN_NAME = "value"

    // ---------------------------------------------------------

    fun generateStatsCreateTableStatement(): String {
        val columns = mutableListOf<String>()
        columns += "$QUEUE_NAME_COLUMN_NAME varchar(${Const.QUEUE_NAME_MAX_LENGTH}) primary key"
        columns += "$LAST_UPDATE_COLUMN_NAME timestamp not null"
        Measure.values().forEach { measure ->
            columns += "${measure.measureName} bigint not null default 0"
        }

        return "create unlogged table if not exists $STATS_TABLE_NAME(${columns.joinToString(separator = ",")})"
    }

    fun generateStatsInsertQuery(queueDump: QueueDump): String {
        val insertColumns = mutableListOf<String>()
        insertColumns += QUEUE_NAME_COLUMN_NAME
        insertColumns += LAST_UPDATE_COLUMN_NAME

        val insertValues = mutableListOf<String>()
        insertValues += "?"                     // queue name
        insertValues += "current_timestamp"     // last_update

        val updateColumns = mutableListOf<String>()
        updateColumns += "$LAST_UPDATE_COLUMN_NAME=current_timestamp"

        queueDump.measures.forEach { measureDump ->
            val measureName = measureDump.measure.measureName

            insertColumns += measureName
            insertValues += "?"
            updateColumns += "$measureName=EXCLUDED.$measureName"
        }

        return """
            insert into $STATS_TABLE_NAME(${insertColumns.joinToString(separator = ",")})
            values (${insertValues.joinToString(separator = ",")})
            on conflict ($QUEUE_NAME_COLUMN_NAME)
            do update set ${updateColumns.joinToString(separator = ",")}
        """.trimIndent()
    }

    fun generateStatsDeleteOutdatedQueuesQuery(days: Int): String {
        return """
            delete from $STATS_TABLE_NAME where
                $LAST_UPDATE_COLUMN_NAME < current_timestamp - interval '$days day'
        """.trimIndent()
    }

    // ---------------------------------------------------------

    const val STATS_INTERNAL_CREATE_TABLE = """
        create unlogged table if not exists $STATS_INTERNAL_TABLE_NAME(
            id bigint generated always as identity (cycle) primary key,
            $QUEUE_NAME_COLUMN_NAME varchar(${Const.QUEUE_NAME_MAX_LENGTH}),
            $MEASURE_NAME_COLUMN_NAME varchar(100),
            $TICK_COLUMN_NAME bigint not null,
            $VALUE_COLUMN_NAME bigint not null
        )
    """

    const val STATS_INTERNAL_INSERT_QUERY = """
        insert into $STATS_INTERNAL_TABLE_NAME
            ($QUEUE_NAME_COLUMN_NAME, $MEASURE_NAME_COLUMN_NAME, $TICK_COLUMN_NAME, $VALUE_COLUMN_NAME)
            values(?, ?, ?, ?)
    """

    const val STATS_INTERNAL_DELETE_OUTDATED_QUERY = """
        delete from $STATS_INTERNAL_TABLE_NAME where
            $MEASURE_NAME_COLUMN_NAME=? and
            $TICK_COLUMN_NAME < ?
    """

    fun generateAggregateQuery(queueDump: QueueDump): String {
        val subSelects = queueDump.measures.map { measureDump ->
            """(
                select sum($VALUE_COLUMN_NAME)
                from $STATS_INTERNAL_TABLE_NAME
                where
                    $QUEUE_NAME_COLUMN_NAME='${queueDump.queue}' and
                    $MEASURE_NAME_COLUMN_NAME='${measureDump.measure.measureName}' and
                    $TICK_COLUMN_NAME>=${measureDump.oldestValidTick}
                ) as ${measureDump.measure.measureName}
            """.trimIndent()
        }

        return "select " + subSelects.joinToString(separator = ",")
    }

}
