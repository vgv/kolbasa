package kolbasa.consumer

import kolbasa.Kolbasa
import kolbasa.consumer.filter.Condition
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.queue.Queue
import kolbasa.stats.GlobalStats
import kolbasa.stats.QueueStats
import kolbasa.stats.prometheus.Consumer
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.LongBox
import kolbasa.utils.TimeHelper
import java.sql.Connection

class ConnectionAwareDatabaseConsumer<Data, Meta : Any>(
    private val queue: Queue<Data, Meta>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions()
) : ConnectionAwareConsumer<Data, Meta> {

    private val queueStats: QueueStats

    init {
        Kolbasa.registerQueue(queue)
        queueStats = GlobalStats.getStatsForQueue(queue)
    }

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
        Consumer.consumerReceiveCounter.labels(queue.name).inc()
        Consumer.consumerReceiveBytesCounter.labels(queue.name).inc(approxBytesCounter.get().toDouble())
        Consumer.consumerReceiveRowsCounter.labels(queue.name).inc(execution.result.toDouble())
        Consumer.consumerReceiveDuration.labels(queue.name).observe(execution.durationSeconds())

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
        Consumer.consumerDeleteCounter.labels(queue.name).inc()
        Consumer.consumerDeleteRowsCounter.labels(queue.name).inc(execution.result.toDouble())
        Consumer.consumerDeleteDuration.labels(queue.name).observe(execution.durationSeconds())

        return execution.result // affected rows
    }

    override fun delete(connection: Connection, message: Message<Data, Meta>): Int {
        return delete(connection, message.id)
    }

    override fun delete(connection: Connection, messages: Collection<Message<Data, Meta>>): Int {
        return delete(connection, messages.map(Message<Data, Meta>::id))
    }
}
