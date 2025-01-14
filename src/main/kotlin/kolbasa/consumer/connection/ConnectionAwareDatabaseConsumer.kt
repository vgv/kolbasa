package kolbasa.consumer.connection

import kolbasa.Kolbasa
import kolbasa.cluster.Shards
import kolbasa.consumer.*
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.Id
import kolbasa.queue.Queue
import kolbasa.stats.prometheus.PrometheusConfig
import kolbasa.stats.prometheus.queuesize.QueueSizeHelper
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.BytesCounter
import kolbasa.utils.TimeHelper
import java.sql.Connection

class ConnectionAwareDatabaseConsumer<Data, Meta : Any> internal constructor(
    private val queue: Queue<Data, Meta>,
    private val consumerOptions: ConsumerOptions = ConsumerOptions(),
    private val interceptors: List<ConnectionAwareConsumerInterceptor<Data, Meta>> = emptyList(),
    private val shards: Shards
) : ConnectionAwareConsumer<Data, Meta> {

    constructor(
        queue: Queue<Data, Meta>,
        consumerOptions: ConsumerOptions = ConsumerOptions(),
        interceptors: List<ConnectionAwareConsumerInterceptor<Data, Meta>> = emptyList()
    ) : this(queue, consumerOptions, interceptors, Shards.ALL_SHARDS)

    override fun receive(connection: Connection, limit: Int, receiveOptions: ReceiveOptions<Meta>): List<Message<Data, Meta>> {
        return ConnectionAwareConsumerInterceptor.recursiveApplyReceiveInterceptors(
            interceptors,
            connection,
            limit,
            receiveOptions
        ) { conn, lmt, rcvOptions ->
            doRealReceive(conn, lmt, rcvOptions)
        }
    }

    private fun doRealReceive(
        connection: Connection,
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>
    ): List<Message<Data, Meta>> {
        // delete expired messages before next read
        if (SweepHelper.needSweep(queue)) {
            SweepHelper.sweep(connection, queue, limit)
        }

        // read
        val approxBytesCounter = BytesCounter(
            (Kolbasa.prometheusConfig as? PrometheusConfig.Config)?.preciseStringSize ?: false
        )

        val query = ConsumerSchemaHelpers.generateSelectPreparedQuery(
            queue, consumerOptions, shards, receiveOptions, limit
        )

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
            approxBytes = approxBytesCounter.get(),
            queueSizeCalcFunc = { QueueSizeHelper.calculateQueueLength(connection, queue) }
        )

        return result
    }

    override fun delete(connection: Connection, messageIds: List<Id>): Int {
        return ConnectionAwareConsumerInterceptor.recursiveApplyDeleteInterceptors(
            interceptors,
            connection,
            messageIds
        ) { conn, msgIds ->
            doRealDelete(conn, msgIds)
        }
    }

    private fun doRealDelete(connection: Connection, messageIds: List<Id>): Int {
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

}
