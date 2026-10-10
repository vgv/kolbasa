package kolbasa.performance

import kolbasa.queue.Queue
import kolbasa.utils.JdbcHelpers.inTransaction
import java.sql.Statement
import javax.sql.DataSource

/**
 * One statement in one transaction, committed at the end, rolled back on failure. Built on kolbasa's own
 * [inTransaction], which is part of its public API - like the examples, the benchmarks use nothing else.
 */
internal fun <T> DataSource.withStatement(block: (Statement) -> T): T =
    inTransaction { connection -> connection.createStatement().use(block) }

/**
 * The physical table name of a queue.
 *
 * TODO: replace with `queue.dbTableName` once that becomes public API.
 *  The library's own rule is `QueueHelpers.generateQueueDbName`: the prefix from
 *  `Const.QUEUE_TABLE_NAME_PREFIX` ("q_") followed by the queue name, lowercased. Both are internal, so
 *  the rule is duplicated here - and if the prefix or the normalisation ever changes, these benchmarks
 *  will quietly truncate the wrong table instead of failing.
 *  Worth exposing on the library side: Architecture.md already tells users that ad-hoc SELECT/UPDATE
 *  against the q_* tables is fine and even encouraged, yet it gives them no way to learn the name.
 */
internal val Queue<*>.tableName: String
    get() = ("q_" + name).lowercase()
