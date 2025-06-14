package kolbasa.consumer.connection

import kolbasa.cluster.Shards
import kolbasa.consumer.*
import kolbasa.consumer.sweep.SweepHelper
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.producer.Id
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import kolbasa.stats.prometheus.queuesize.QueueSizeHelper
import kolbasa.stats.sql.SqlDumpHelper
import kolbasa.stats.sql.StatementKind
import kolbasa.utils.BytesCounter
import kolbasa.utils.TimeHelper
import java.sql.Connection

class ConnectionAwareDatabaseConsumer internal constructor(
    private val nodeId: NodeId,
    private val consumerOptions: ConsumerOptions,
    private val shards: Shards
) : ConnectionAwareConsumer {

    @JvmOverloads
    constructor(consumerOptions: ConsumerOptions = ConsumerOptions()) : this(
        nodeId = NodeId.EMPTY_NODE_ID,
        consumerOptions = consumerOptions,
        shards = Shards.ALL_SHARDS
    )

    override fun <Data, Meta : Any> receive(
        connection: Connection,
        queue: Queue<Data, Meta>,
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>
    ): List<Message<Data, Meta>> {
        // Do we need to read OT data?
        receiveOptions.readOpenTelemetryData = queue.queueTracing.readOpenTelemetryData()

        return queue.queueTracing.makeConsumerCall {
            doRealReceive(connection, queue, limit, receiveOptions)
        }
    }

    private fun <Data, Meta : Any> doRealReceive(
        connection: Connection,
        queue: Queue<Data, Meta>,
        limit: Int,
        receiveOptions: ReceiveOptions<Meta>
    ): List<Message<Data, Meta>> {
        // delete expired messages before next read
        if (SweepHelper.needSweep(queue)) {
            SweepHelper.sweep(connection, queue, nodeId, limit)
        }

        // read
        val approxBytesCounter = BytesCounter(queue.queueMetrics.usePreciseStringSize())

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
            nodeId = nodeId,
            receivedMessages = result.size,
            executionNanos = execution.durationNanos,
            approxBytes = approxBytesCounter.get(),
            queueSizeCalcFunc = { QueueSizeHelper.calculateQueueLength(connection, queue) }
        )

        return result
    }

    override fun <Data, Meta : Any> delete(connection: Connection, queue: Queue<Data, Meta>, messageIds: List<Id>): Int {
        if (messageIds.isEmpty()) {
            return 0
        }

        val deleteQuery = ConsumerSchemaHelpers.generateDeleteQuery(queue, messageIds)
        val (execution, removedMessages) = TimeHelper.measure {
            connection.useStatement { statement ->
                statement.executeUpdate(deleteQuery)
            }
        }

        // SQL Dump
        SqlDumpHelper.dumpQuery(queue, StatementKind.CONSUMER_DELETE, deleteQuery, execution, removedMessages)

        // Prometheus
        queue.queueMetrics.consumerDeleteMetrics(
            nodeId = nodeId,
            removedMessages = removedMessages,
            executionNanos = execution.durationNanos
        )

        return removedMessages
    }
}
