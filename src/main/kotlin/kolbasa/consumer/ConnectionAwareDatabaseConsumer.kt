package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.Queue
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.BytesCounter
import kolbasa.utils.TimeHelper
import java.sql.Connection

class ConnectionAwareDatabaseConsumer<Data, Meta : Any>(
    private val queue: Queue<Data, Meta>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions()
) : ConnectionAwareConsumer<Data, Meta> {

    override fun receive(connection: Connection): Message<Data, Meta>? {
        return receive(connection, ReceiveOptions())
    }

    override fun receive(connection: Connection, filter: () -> Condition<Meta>): Message<Data, Meta>? {
        return receive(connection, ReceiveOptions(filter = filter()))
    }

    override fun receive(connection: Connection, receiveOptions: ReceiveOptions<Meta>): Message<Data, Meta>? {
        val result = receive(connection, limit = 1, receiveOptions)
        return result.firstOrNull()
    }

    override fun receive(connection: Connection, limit: Int): List<Message<Data, Meta>> {
        return receive(connection, limit, ReceiveOptions())
    }

    override fun receive(connection: Connection, limit: Int, filter: () -> Condition<Meta>): List<Message<Data, Meta>> {
        return receive(connection, limit, ReceiveOptions(filter = filter()))
    }

    override fun receive(connection: Connection, limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        // delete expired messages before next read
        if (SweepHelper.needSweep(queue)) {
            SweepHelper.sweep(connection, queue, limit)
        }

        // read
        val approxBytesCounter = BytesCounter()
        val query = ConsumerSchemaHelpers.generateSelectPreparedQuery(queue, consumerOptions, receiveOptions, limit)

        val (execution, result) = TimeHelper.measure {
            connection.prepareStatement(query).use { preparedStatement ->
                ConsumerSchemaHelpers.fillSelectPreparedQuery(queue, consumerOptions, receiveOptions, preparedStatement)
                preparedStatement.executeQuery().use { resultSet ->
                    val result = ArrayList<Message<Data, Meta>>(limit)

                    while (resultSet.next()) {
                        result += ConsumerSchemaHelpers.read(queue, receiveOptions, resultSet, approxBytesCounter)
                    }

                    result
                }
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.CONSUMER_SELECT, query, execution, result.size)

        // Prometheus
        queue.queueMetrics.consumerReceiveMetrics(
            receivedRows = result.size,
            executionNanos = execution.durationNanos,
            approxBytes = approxBytesCounter.get()
        )

        return result
    }

    override fun delete(connection: Connection, messageId: Long): Int {
        return delete(connection, listOf(messageId))
    }

    override fun delete(connection: Connection, messageIds: List<Long>): Int {
        if (messageIds.isEmpty()) {
            return 0
        }

        val deleteQuery = ConsumerSchemaHelpers.generateDeleteQuery(queue, messageIds)
        val (execution, removedRows) = TimeHelper.measure {
            connection.useStatement { statement ->
                statement.executeUpdate(deleteQuery)
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.CONSUMER_DELETE, deleteQuery, execution, removedRows)

        // Prometheus
        queue.queueMetrics.consumerDeleteMetrics(removedRows, execution.durationNanos)

        return removedRows
    }

    override fun delete(connection: Connection, message: Message<Data, Meta>): Int {
        return delete(connection, message.id)
    }

    override fun delete(connection: Connection, messages: Collection<Message<Data, Meta>>): Int {
        return delete(connection, messages.map(Message<Data, Meta>::id))
    }
}
