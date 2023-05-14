package kolbasa.pg

import kolbasa.pg.DatabaseExtensions.readBoolean
import kolbasa.pg.DatabaseExtensions.useConnection
import kolbasa.pg.DatabaseExtensions.useStatement
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import javax.sql.DataSource

internal object Lock {

    fun <R> tryRunExclusive(dataSource: DataSource, lockId: Long, block: (Connection) -> R): R? {
        // We try to acquire JVM lock before PG lock
        // It's just an optimization for cases, when we want to acquire the same lock within one JVM
        // It is just much faster to try to acquire JVM lock before PG
        return jvmTryRunExclusive(lockId) {
            pgTryRunExclusive(dataSource, lockId, block)
        }
    }

    fun <R> tryRunExclusive(connection: Connection, lockId: Long, block: (Connection) -> R): R? {
        // We try to acquire JVM lock before PG lock
        // It's just an optimization for cases, when we want to acquire the same lock within one JVM
        // It is just much faster to try to acquire JVM lock before PG
        return jvmTryRunExclusive(lockId) {
            pgTryRunExclusive(connection, lockId, block)
        }
    }

    private fun <R> jvmTryRunExclusive(lockId: Long, block: () -> R): R? {
        val lock = jvmLocks.computeIfAbsent(lockId) { _ ->
            ReentrantLock()
        }

        return if (lock.tryLock()) {
            try {
                block()
            } finally {
                lock.unlock()
            }
        } else {
            null
        }
    }

    private fun <R> pgTryRunExclusive(dataSource: DataSource, lockId: Long, block: (Connection) -> R): R? {
        return dataSource.useConnection { connection ->
            pgTryRunExclusive(connection, lockId, block)
        }
    }

    private fun <R> pgTryRunExclusive(connection: Connection, lockId: Long, block: (Connection) -> R): R? {
        return if (pgTryLock(connection, lockId)) {
            try {
                block(connection)
            } finally {
                pgUnlock(connection, lockId)
            }
        } else {
            null
        }
    }

    private fun pgTryLock(connection: Connection, lockId: Long): Boolean {
        val query = "select * from pg_try_advisory_lock($lockId)"
        return connection.readBoolean(query)
    }

    private fun pgUnlock(connection: Connection, lockId: Long) {
        val query = "select * from pg_advisory_unlock($lockId)"
        connection.useStatement { statement ->
            statement.execute(query)
        }
    }

    private val jvmLocks = ConcurrentHashMap<Long, Lock>()

}
