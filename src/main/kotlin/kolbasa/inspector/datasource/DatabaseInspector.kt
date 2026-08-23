package kolbasa.inspector.datasource

import kolbasa.inspector.CountOptions
import kolbasa.inspector.DistinctValuesOptions
import kolbasa.inspector.MessageAge
import kolbasa.inspector.Messages
import kolbasa.inspector.connection.ConnectionAwareDatabaseInspector
import kolbasa.inspector.connection.ConnectionAwareInspector
import kolbasa.queue.Queue
import kolbasa.queue.meta.MetaField
import kolbasa.utils.JdbcHelpers.useConnection
import javax.sql.DataSource

/**
 * Default implementation of [Inspector]
 */
class DatabaseInspector(
    private val dataSource: DataSource,
    private val peer: ConnectionAwareInspector
) : Inspector {

    /**
     * Creates an inspector that manages connections itself.
     *
     * Every call takes a connection from `dataSource`, reads what it needs and gives the connection back. The
     * inspector only reads: it never changes a queue or a message.
     *
     * The inspector is thread-safe and holds no state between calls, so create one and share it.
     *
     * @param dataSource the pool this inspector takes connections from
     */
    constructor(dataSource: DataSource) : this(
        dataSource = dataSource,
        peer = ConnectionAwareDatabaseInspector()
    )

    override fun count(queue: Queue<*>, options: CountOptions): Messages {
        return dataSource.useConnection { connection ->
            peer.count(connection, queue, options)
        }
    }

    override fun <V> distinctValues(
        queue: Queue<*>,
        metaField: MetaField<V>,
        limit: Int,
        options: DistinctValuesOptions
    ): Map<V?, Long> {
        return dataSource.useConnection { connection ->
            peer.distinctValues(connection, queue, metaField, limit, options)
        }
    }

    override fun size(queue: Queue<*>): Long {
        return dataSource.useConnection { connection ->
            peer.size(connection, queue)
        }
    }

    override fun isEmpty(queue: Queue<*>): Boolean {
        return dataSource.useConnection { connection ->
            peer.isEmpty(connection, queue)
        }
    }

    override fun isDeadOrEmpty(queue: Queue<*>): Boolean {
        return dataSource.useConnection { connection ->
            peer.isDeadOrEmpty(connection, queue)
        }
    }

    override fun messageAge(queue: Queue<*>): MessageAge {
        return dataSource.useConnection { connection ->
            peer.messageAge(connection, queue)
        }
    }

}
