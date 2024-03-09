package kolbasa.stats.prometheus.queuesize

import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.Queue
import java.sql.Connection
import java.sql.Statement

internal object QueueSizeHelper {

    /**
     * Calculate queue length using PostgreSQL statistics
     * This is a very fast and efficient way to calculate table size with only one drawback – it's not 100% accurate. The
     * result is approximate and can be far from perfection if the table has never been vacuumed or vacuumed very rarely.
     *
     * However, for queueing purposes it's ok to use this method, because queues usually have predictable
     * workload (send/receive rate) with average record size (for particular queue) and vacuuming is a regular operation.
     * It's pretty extreme to have a queue with 10 billion inserts one day in a month and 0 inserts the rest of the
     * month. So, if you have an extreme case, you need an extreme solution, for example – do vacuum on a manual basis
     * right after the peak load.
     *
     * @param connection - connection to the database
     * @param queue - queue to measure
     * @return
     * - [Const.TABLE_HAS_NEVER_BEEN_VACUUMED] if queue table has never been vacuumed
     * - [Const.TABLE_DOES_NOT_EXIST] if table doesn't exist
     * - otherwise approximate queue length
     */
    fun calculateQueueLength(connection: Connection, queue: Queue<*, *>): Long {
        val tableSizeData = readTableSizeData(connection, queue.dbTableName)

        return tableSizeData?.getTableSize() ?: Const.TABLE_DOES_NOT_EXIST
    }

    private fun readTableSizeData(connection: Connection, tableName: String): TableSizeData? {
        val sql = """
            select
                reltuples::bigint,
                relpages::int,
                pg_catalog.pg_relation_size(oid)::bigint,
                pg_catalog.current_setting('block_size')::int
            from pg_catalog.pg_class
            where
                oid = to_regclass(current_schema() || '.$tableName')
        """.trimIndent()

        return connection.useStatement { statement: Statement ->
            statement.executeQuery(sql).use { rs ->
                if (rs.next()) {
                    val reltuples = rs.getLong(1)
                    val relpages = rs.getInt(2)
                    val relsize = rs.getLong(3)
                    val blocksize = rs.getInt(4)

                    TableSizeData(reltuples, relpages, relsize, blocksize)
                } else {
                    null
                }
            }
        }
    }

    private data class TableSizeData(
        val lastKnownRecords: Long,
        val lastKnownPages: Int,
        val databaseSize: Long,
        val blockSize: Int
    ) {
        fun getTableSize(): Long {
            if (lastKnownRecords == -1L) {
                // Table has never been vacuumed, so we cannot even approximately calculate the size
                // We return -1 as a signal that we don't know the size, so, caller should not trust the result
                return Const.TABLE_HAS_NEVER_BEEN_VACUUMED
            }

            if (lastKnownPages == 0) {
                // According to the latest vacuum stats the table is empty
                return 0
            }

            // Everything looks fine, let's calculate approximate size
            val averageRecordsPerPage = lastKnownRecords.toDouble() / lastKnownPages
            val realPages = databaseSize.toDouble() / blockSize
            return (realPages * averageRecordsPerPage).toLong()
        }
    }

}
