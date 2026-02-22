package kolbasa.inspector.datasource

import kolbasa.inspector.CountOptions
import kolbasa.inspector.DistinctValuesOptions
import kolbasa.inspector.MessageAge
import kolbasa.inspector.Messages
import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField

/**
 * Provides read-only inspection capabilities for queues.
 *
 * Allows querying queue statistics such as message counts by state, approximate queue size (in bytes),
 * distinct meta-field values and message age information.
 */
interface Inspector {

    /**
     * Counts messages in the queue grouped by their state (`SCHEDULED`, `READY`, `IN_FLIGHT`, `RETRY`, `DEAD`).
     *
     * This method is designed to be called frequently — for example, every minute as part of monitoring — so it
     * needs to be fast. A full table scan would be too expensive for large tables, so instead this method uses
     * PostgreSQL [TABLESAMPLE](https://www.postgresql.org/docs/18/sql-select.html#SQL-FROM) to read only a
     * fraction of the table. This means the result is an approximation, not an exact count.
     *
     * The sampling percent is adjustable in the range (0, 100] via [CountOptions.samplePercent]. You can pick a
     * value that balances speed and accuracy for your use case — or just leave it at
     * [CountOptions.YOU_KNOW_BETTER][kolbasa.inspector.CountOptions.YOU_KNOW_BETTER] (the default)
     * and let Kolbasa choose a reasonable sampling level for you based on the table size.
     *
     * You can also provide an optional filter condition [CountOptions.filter] to restrict which messages are counted.
     *
     * @param queue the queue to inspect
     * @param options counting options such as sample percent and an optional filter condition
     * @return a [Messages] instance with per-state counts
     */
    fun count(queue: Queue<*>, options: CountOptions = CountOptions.DEFAULT): Messages

    /**
     * Returns distinct values of a meta-field across messages in the queue.
     *
     * Useful for discovering what meta-field values are currently present in the queue — for example, to see
     * which tenants/shards/partitions have pending messages for load distribution or debugging.
     *
     * Just like [count], this method uses PostgreSQL
     * [TABLESAMPLE](https://www.postgresql.org/docs/18/sql-select.html#SQL-FROM) to avoid a full table scan,
     * so the result is an approximation — some rare values may be missing from the sample.
     *
     * The sampling percent is adjustable in the range (0, 100] via [DistinctValuesOptions.samplePercent]. You can
     * pick a value that balances speed and completeness for your use case — or just leave it at
     * [DistinctValuesOptions.YOU_KNOW_BETTER][kolbasa.inspector.DistinctValuesOptions.YOU_KNOW_BETTER] (the default)
     * and let Kolbasa choose a reasonable sampling level for you based on the table size.
     *
     * You can also provide an optional filter condition [DistinctValuesOptions.filter] to restrict which messages are counted.
     *
     * Results can be sorted by count using [DistinctValuesOptions.order].
     *
     * @param queue the queue to inspect
     * @param metaField the meta-field whose distinct values to retrieve
     * @param limit maximum number of distinct values to return
     * @param options options such as sample percent, an optional filter condition and sort order
     * @return a map of distinct values to their (approximate) counts (may contain `null` key because meta-field may be missing)
     */
    fun <V> distinctValues(
        queue: Queue<*>,
        metaField: MetaField<V>,
        limit: Int,
        options: DistinctValuesOptions = DistinctValuesOptions.DEFAULT
    ): Map<V?, Long>

    /**
     * Returns the total size of the queue table in bytes, including indexes, TOAST data, and other
     * associated storage.
     *
     * This calculation is very fast and does not depend on the size of the table.
     *
     * @param queue the queue to inspect
     * @return total size in bytes
     */
    fun size(queue: Queue<*>): Long

    /**
     * Checks whether the queue has no messages in any state.
     *
     * This method is very fast — it stops at the first row found and does not scan the whole table — so it is safe
     * to call frequently, for example as part of a tight polling loop or a health check.
     *
     * @param queue the queue to inspect
     * @return `true` if the queue contains no messages at all, `false` otherwise
     */
    fun isEmpty(queue: Queue<*>): Boolean

    /**
     * Checks whether the queue has no live messages (i.e., is either empty or contains only dead messages).
     *
     * Unlike [isEmpty], this method returns `true` if the queue contains only [dead][kolbasa.queue.meta.FieldOption]
     * messages — messages that have exhausted all retry attempts and will not be processed again.
     *
     * This method scans the table looking for the first live message and stops as soon as it finds one, so, in general it is
     * as fast as [isEmpty]. However, if the queue contains only dead messages, there is nothing to stop at and the scan must
     * read the entire table — the pathological case. On a large tables (gigabytes) this can be costly, so be careful about
     * calling this method frequently in that pathological scenario (large table contains only dead messages).
     *
     * Running a sweep process that regularly removes dead messages will prevent this situation from arising.
     *
     * @param queue the queue to inspect
     * @return `true` if the queue contains no messages, or only dead messages; `false` otherwise
     */
    fun isDeadOrEmpty(queue: Queue<*>): Boolean

    /**
     * Returns age information about messages in the queue.
     *
     * To find the oldest message ready to be processed, this method performs an index scan looking for the first
     * `READY` or `RETRY` message. If such a message exists, the scan stops immediately. If there are no `READY` or
     * `RETRY` messages, however, the index scan naturally degrades into a full scan. On a large table this can be
     * time-consuming — but the performance is no worse than a single
     * [Consumer.receive()][kolbasa.consumer.datasource.Consumer.receive] call, which the application is already
     * making constantly anyway.
     *
     * @param queue the queue to inspect
     * @return a [MessageAge] with the age of the oldest, newest, and oldest ready message
     */
    fun messageAge(queue: Queue<*>): MessageAge

}
