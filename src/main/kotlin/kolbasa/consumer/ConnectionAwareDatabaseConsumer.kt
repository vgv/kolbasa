package kolbasa.consumer

import kolbasa.Kolbasa
import kolbasa.consumer.filter.Condition
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.Queue
import kolbasa.stats.GlobalStats
import kolbasa.stats.QueueStats
import kolbasa.utils.LongBox
import java.sql.Connection

class ConnectionAwareDatabaseConsumer<V, M : Any>(
    private val queue: Queue<V, M>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions()
) : ConnectionAwareConsumer<V, M> {

    private val queueStats: QueueStats

    init {
        Kolbasa.registerQueue(queue)
        queueStats = GlobalStats.getStatsForQueue(queue)
    }

    override fun receive(connection: Connection): Message<V, M>? {
        return receive(connection, ReceiveOptions())
    }

    override fun receive(connection: Connection, filter: () -> Condition<M>): Message<V, M>? {
        return receive(connection, ReceiveOptions(filter = filter()))
    }

    override fun receive(connection: Connection, receiveOptions: ReceiveOptions<M>): Message<V, M>? {
        val result = receive(connection, limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    override fun receive(connection: Connection, limit: Int): List<Message<V, M>> {
        return receive(connection, limit, ReceiveOptions())
    }

    override fun receive(connection: Connection, limit: Int, filter: () -> Condition<M>): List<Message<V, M>> {
        return receive(connection, limit, ReceiveOptions(filter = filter()))
    }

    override fun receive(connection: Connection, limit: Int, receiveOptions: ReceiveOptions<M>): List<Message<V, M>> {
        // delete expired messages before next read
        SweepHelper.sweep(connection, queue)

        // read
        val query = ConsumerSchemaHelpers.generateSelectPreparedQuery(queue, consumerOptions, receiveOptions, limit)
        return connection.prepareStatement(query).use { preparedStatement ->
            ConsumerSchemaHelpers.fillSelectPreparedQuery(queue, consumerOptions, receiveOptions, preparedStatement)
            preparedStatement.executeQuery().use { resultSet ->
                val approxBytesCounter = LongBox()
                val result = ArrayList<Message<V, M>>(limit)

                while (resultSet.next()) {
                    result += ConsumerSchemaHelpers.read(queue, receiveOptions, resultSet, approxBytesCounter)
                }

                queueStats.receiveInc(result.size.toLong(), approxBytesCounter.get())
                result
            }
        }
    }

    override fun delete(connection: Connection, messageId: Long): Int {
        val query = ConsumerSchemaHelpers.generateDeleteQuery(queue, messageId)
        return connection.useStatement { statement ->
            statement.executeUpdate(query)
        }
    }

    override fun delete(connection: Connection, messageIds: List<Long>): Int {
        if (messageIds.isEmpty()) {
            return 0
        }

        val query = ConsumerSchemaHelpers.generateDeleteQuery(queue, messageIds)
        return connection.useStatement { statement ->
            statement.executeUpdate(query)
        }
    }

    override fun delete(connection: Connection, message: Message<V, M>): Int {
        return delete(connection, message.id)
    }

    override fun delete(connection: Connection, messages: Collection<Message<V, M>>): Int {
        return delete(connection, messages.map(Message<V, M>::id))
    }
}
