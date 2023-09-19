package kolbasa.consumer

import kolbasa.consumer.filter.Condition
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.Queue
import kolbasa.stats.prometheus.PrometheusConsumer
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.LongBox
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
        val approxBytesCounter = LongBox()
        val result = ArrayList<Message<Data, Meta>>(limit)
        val query = ConsumerSchemaHelpers.generateSelectPreparedQuery(queue, consumerOptions, receiveOptions, limit)

        val execution = TimeHelper.measure {
            connection.prepareStatement(query).use { preparedStatement ->
                ConsumerSchemaHelpers.fillSelectPreparedQuery(queue, consumerOptions, receiveOptions, preparedStatement)
                preparedStatement.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        result += ConsumerSchemaHelpers.read(queue, receiveOptions, resultSet, approxBytesCounter)
                    }

                    result.size
                }
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.CONSUMER_SELECT, query, execution)

        // Prometheus
        PrometheusConsumer.consumerReceiveCounter.labels(queue.name).inc()
        PrometheusConsumer.consumerReceiveBytesCounter.labels(queue.name).inc(approxBytesCounter.get().toDouble())
        PrometheusConsumer.consumerReceiveRowsCounter.labels(queue.name).inc(execution.result.toDouble())
        PrometheusConsumer.consumerReceiveDuration.labels(queue.name).observe(execution.durationSeconds())

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
        val execution = TimeHelper.measure {
            connection.useStatement { statement ->
                statement.executeUpdate(deleteQuery)
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.CONSUMER_DELETE, deleteQuery, execution)

        // Prometheus
        PrometheusConsumer.consumerDeleteCounter.labels(queue.name).inc()
        PrometheusConsumer.consumerDeleteRowsCounter.labels(queue.name).inc(execution.result.toDouble())
        PrometheusConsumer.consumerDeleteDuration.labels(queue.name).observe(execution.durationSeconds())

        return execution.result // affected rows
    }

    override fun delete(connection: Connection, message: Message<Data, Meta>): Int {
        return delete(connection, message.id)
    }

    override fun delete(connection: Connection, messages: Collection<Message<Data, Meta>>): Int {
        return delete(connection, messages.map(Message<Data, Meta>::id))
    }
}
