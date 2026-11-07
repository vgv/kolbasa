package kolbasa.telemetry.sql

import kolbasa.Kolbasa
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.consumer.sweep.SweepHelper
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.producer.DeduplicationMode
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.*
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.NodeId
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.inTransaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Isolated
import java.io.StringWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

// Kolbasa.sqlDumpConfig is global, so no other test may run at the same time
@Isolated
class SqlDumpTest : AbstractPostgreSQLTest() {

    // Everything the dump writes during one test
    private val writer = StringWriter()

    private val sweepConfig = Kolbasa.sweepConfig

    @BeforeEach
    fun disableSweep() {
        // A sweep inside receive() would add its own records
        Kolbasa.sweepConfig = SweepConfig(probability = SweepConfig.SWEEP_IS_DISABLED)
    }

    @AfterEach
    fun resetConfig() {
        Kolbasa.sqlDumpConfig = SqlDumpConfig()
        Kolbasa.sweepConfig = sweepConfig
    }

    @Test
    fun testRecordFormat() {
        val queue = createQueue("sql_dump_format")
        dump(queue, StatementKind.PRODUCER_INSERT)

        val before = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS)
        DatabaseProducer(dataSource).send(queue, "first")
        val after = LocalDateTime.now()

        // records() checks the layout of every record, this test checks the values
        val record = records().single()
        assertTrue(record.date in before..after) { "${record.date} is not between $before and $after" }
        assertEquals("sql_dump_format", record.queue)
        assertNull(record.node)
        assertEquals(1, record.rows)
        assertEquals(StatementKind.PRODUCER_INSERT, record.kind)
        assertTrue(record.sql.startsWith("insert into q_sql_dump_format("), record.sql)
    }

    @Test
    fun testNotEnabled() {
        val queue = createQueue("sql_dump_not_enabled")

        // The writer and the queue are set, but enable() is not called
        Kolbasa.sqlDumpConfig = SqlDumpConfig.builder()
            .writer(writer)
            .queue(queue, *StatementKind.entries.toTypedArray())
            .build()

        DatabaseProducer(dataSource).send(queue, "first")

        assertEquals(emptyList<Record>(), records())
    }

    @Test
    fun testSendReceiveDelete() {
        val queue = createQueue("sql_dump_send_receive_delete")
        dump(queue, *StatementKind.entries.toTypedArray())

        DatabaseProducer(dataSource).send(queue, "first")
        val consumer = DatabaseConsumer(dataSource)
        consumer.delete(queue, requireNotNull(consumer.receive(queue)))

        val expected = listOf(
            StatementKind.PRODUCER_INSERT to 1,
            StatementKind.CONSUMER_SELECT to 1,
            StatementKind.CONSUMER_DELETE to 1
        )
        assertEquals(expected, records().map { it.kind to it.rows })
    }

    @Test
    fun testChosenKinds() {
        val queue = createQueue("sql_dump_chosen_kinds")
        dump(queue, StatementKind.CONSUMER_SELECT)

        DatabaseProducer(dataSource).send(queue, "first")
        val consumer = DatabaseConsumer(dataSource)
        consumer.delete(queue, requireNotNull(consumer.receive(queue)))

        // Only the receive is printed
        assertEquals(listOf(StatementKind.CONSUMER_SELECT), records().map { it.kind })
    }

    @Test
    fun testChosenQueues() {
        val first = createQueue("sql_dump_first_queue")
        val second = createQueue("sql_dump_second_queue")
        dump(first, *StatementKind.entries.toTypedArray())

        val producer = DatabaseProducer(dataSource)
        producer.send(first, "first")
        producer.send(second, "second")

        // Only the statement for the first queue is printed
        assertEquals(listOf("sql_dump_first_queue"), records().map { it.queue })
    }

    @Test
    fun testNode() {
        val queue = createQueue("sql_dump_node")
        dump(queue, StatementKind.PRODUCER_INSERT)

        // A producer of a cluster works with one node, and the record names it
        val producer = ConnectionAwareDatabaseProducer(NodeId("node_1"), ProducerOptions())
        dataSource.inTransaction { connection -> producer.send(connection, queue, "first") }

        assertEquals("node_1", records().single().node)
    }

    @Test
    fun testChunks() {
        val queue = createQueue("sql_dump_chunks")
        dump(queue, StatementKind.PRODUCER_INSERT)

        // 5 messages in chunks of 2: three statements
        val producer = DatabaseProducer(dataSource, ProducerOptions(chunkSize = 2))
        producer.send(queue, (1..5).map { SendMessage("message $it") })

        assertEquals(listOf(2, 2, 1), records().map { it.rows })
    }

    @Test
    fun testInsertRowsWithoutDuplicates() {
        val field = MetaField.ofInt("field", FieldOption.ALL_LIVE_UNIQUE)
        val queue = Queue.of("sql_dump_duplicates", PredefinedDataTypes.String, Metadata.of(field))
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
        dump(queue, StatementKind.PRODUCER_INSERT)

        // The second message is a duplicate of the first: it is not inserted, so it is not a row
        val producer = DatabaseProducer(dataSource, ProducerOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATE))
        val message = SendMessage("first", MetaValues.of(field.value(1)))
        producer.send(queue, message)
        producer.send(queue, message)

        assertEquals(listOf(1, 0), records().map { it.rows })
    }

    @Test
    fun testArchive() {
        val queue = createQueue("sql_dump_archive", QueueOptions(archiveQueueOptions = ArchiveQueueOptions()))
        dump(queue, *StatementKind.entries.toTypedArray())

        DatabaseProducer(dataSource).send(queue, "first")
        val consumer = DatabaseConsumer(dataSource)
        consumer.delete(queue, requireNotNull(consumer.receive(queue)))

        // With an archive, delete moves the message there
        val expected = listOf(
            StatementKind.PRODUCER_INSERT to 1,
            StatementKind.CONSUMER_SELECT to 1,
            StatementKind.CONSUMER_DELETE_TO_ARCHIVE to 1
        )
        assertEquals(expected, records().map { it.kind to it.rows })
    }

    @Test
    fun testSweep() {
        val queue = createQueue("sql_dump_sweep")
        dump(queue, *StatementKind.entries.toTypedArray())

        // Nothing to remove, but the statement runs and is printed
        dataSource.inTransaction { connection -> SweepHelper.sweep(connection, queue, 100) }

        assertEquals(listOf(StatementKind.SWEEP to 0), records().map { it.kind to it.rows })
    }

    @Test
    fun testSweepToDlq() {
        val queue = createQueue("sql_dump_dlq_sweep", QueueOptions(dlqOptions = DlqOptions()))
        dump(queue, *StatementKind.entries.toTypedArray())

        dataSource.inTransaction { connection -> SweepHelper.sweep(connection, queue, 100) }

        // The retention cleanup of the DLQ is a step of the same sweep
        val expected = listOf(
            StatementKind.SWEEP_TO_DLQ to 0,
            StatementKind.RETENTION_CLEANUP to 0
        )
        assertEquals(expected, records().map { it.kind to it.rows })
    }

    @Test
    fun testRetentionCleanup() {
        val options = QueueOptions(dlqOptions = DlqOptions(), archiveQueueOptions = ArchiveQueueOptions())
        val queue = createQueue("sql_dump_retention", options)

        // The retention cleanup is printed for the main queue, as a step of its sweep
        dump(queue, StatementKind.RETENTION_CLEANUP)

        dataSource.inTransaction { connection -> SweepHelper.sweep(connection, queue, 100) }

        // One statement for the DLQ, one for the archive. The SQL names their tables
        val records = records()
        assertEquals(listOf("sql_dump_retention", "sql_dump_retention"), records.map { it.queue })
        assertTrue("q_sql_dump_retention_dlq" in records[0].sql, records[0].sql)
        assertTrue("q_sql_dump_retention_arc" in records[1].sql, records[1].sql)
    }

    @Test
    fun testDlqIsItsOwnQueue() {
        val queue = createQueue("sql_dump_dlq_reader", QueueOptions(dlqOptions = DlqOptions()))
        val dlq = requireNotNull(queue.deadLetterQueue)
        dump(dlq, *StatementKind.entries.toTypedArray())

        // The main queue is not registered, the DLQ is
        DatabaseProducer(dataSource).send(queue, "first")
        DatabaseConsumer(dataSource).receive(dlq)

        assertEquals(listOf(StatementKind.CONSUMER_SELECT to "sql_dump_dlq_reader_dlq"), records().map { it.kind to it.queue })
    }

    @Test
    fun testMutateById() {
        val queue = createQueue("sql_dump_mutate_by_id")
        val id = DatabaseProducer(dataSource).send(queue, "first").onlySuccessful().single().id
        dump(queue, *StatementKind.entries.toTypedArray())

        DatabaseMutator(dataSource).addRemainingAttempts(queue, 1, id)

        assertEquals(listOf(StatementKind.MUTATE_BY_ID to 1), records().map { it.kind to it.rows })
    }

    @Test
    fun testMutateByFilter() {
        val field = MetaField.ofInt("field", FieldOption.SEARCH)
        val queue = Queue.of("sql_dump_mutate_by_filter", PredefinedDataTypes.String, Metadata.of(field))
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
        DatabaseProducer(dataSource).send(queue, SendMessage("first", MetaValues.of(field.value(1))))
        dump(queue, *StatementKind.entries.toTypedArray())

        DatabaseMutator(dataSource).mutate(queue, listOf(AddRemainingAttempts(1))) { field eq 1 }

        // One statement per iteration: the first changes the message, the second finds nothing more
        val expected = listOf(
            StatementKind.MUTATE_BY_FILTER to 1,
            StatementKind.MUTATE_BY_FILTER to 0
        )
        assertEquals(expected, records().map { it.kind to it.rows })
    }

    private fun createQueue(name: String, options: QueueOptions = QueueOptions()): Queue<String> {
        val queue = Queue(name = name, databaseDataType = PredefinedDataTypes.String, options = options)
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
        return queue
    }

    /**
     * Turns the dump on for [queue] and [kinds], into the [writer] of this test.
     */
    private fun dump(queue: Queue<*>, vararg kinds: StatementKind) {
        Kolbasa.sqlDumpConfig = SqlDumpConfig.builder()
            .enable()
            .writer(writer)
            .queue(queue, *kinds)
            .build()
    }

    private data class Record(
        val date: LocalDateTime,
        val queue: String,
        val node: String?,
        val kind: StatementKind,
        val durationMillis: Double,
        val rows: Int,
        val sql: String
    )

    /**
     * The records the dump wrote in this test, in order. Fails if a record doesn't follow the format.
     */
    private fun records(): List<Record> {
        val text = writer.toString()
        assertTrue(text.isEmpty() || text.startsWith(SEPARATOR + "\n")) { "The dump doesn't start with a record: $text" }

        return text.split(SEPARATOR + "\n")
            .drop(1)
            .map { record ->
                val match = requireNotNull(RECORD.matchEntire(record)) { "Not a record: $record" }
                val (date, queue, node, kind, duration, rows, sql) = match.destructured
                Record(
                    date = LocalDateTime.parse(date, DATE_FORMAT),
                    queue = queue,
                    node = node.ifEmpty { null },
                    kind = StatementKind.valueOf(kind),
                    durationMillis = duration.toDouble(),
                    rows = rows.toInt(),
                    sql = sql
                )
            }
    }

    private companion object {
        val SEPARATOR = "-".repeat(45)

        // One record after the separator: the fields, one per line, then the SQL. Node is only there in a cluster
        val RECORD = Regex(
            "Date: ([^\n]+)\nQueue: ([^\n]+)\n(?:Node: ([^\n]+)\n)?" +
                "Kind: (\\w+)\nDuration: (\\d+\\.\\d{3})ms\nRows: (\\d+)\n(.*)\n",
            RegexOption.DOT_MATCHES_ALL
        )

        val DATE_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    }
}
