package kolbasa.telemetry.sql

import kolbasa.Kolbasa
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.consumer.sweep.SweepConfig
import kolbasa.consumer.sweep.SweepHelper
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.datasource.DatabaseMutator
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.*
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.SchemaHelpers
import kolbasa.test.AbstractPostgreSQLTest
import kolbasa.utils.JdbcHelpers.inTransaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
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
        val records = records()
        assertEquals(1, records.size)
        assertTrue("q_sql_dump_first_queue" in records.single().sql)
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

        // The retention cleanup of the DLQ runs too, but it is not printed for the main queue
        assertEquals(listOf(StatementKind.SWEEP_TO_DLQ to 0), records().map { it.kind to it.rows })
    }

    @Test
    fun testRetentionCleanup() {
        val queue = createQueue("sql_dump_retention", QueueOptions(dlqOptions = DlqOptions()))

        // The retention cleanup is printed for the DLQ itself
        dump(requireNotNull(queue.deadLetterQueue), *StatementKind.entries.toTypedArray())

        dataSource.inTransaction { connection -> SweepHelper.sweep(connection, queue, 100) }

        val records = records()
        assertEquals(listOf(StatementKind.RETENTION_CLEANUP to 0), records.map { it.kind to it.rows })
        assertTrue("q_sql_dump_retention_dlq" in records.single().sql)
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
        val durationMillis: Long,
        val rows: Int,
        val kind: StatementKind,
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
                val (date, duration, rows, kind, sql) = match.destructured
                Record(
                    date = LocalDateTime.parse(date, DATE_FORMAT),
                    durationMillis = duration.toLong(),
                    rows = rows.toInt(),
                    kind = StatementKind.valueOf(kind),
                    sql = sql
                )
            }
    }

    private companion object {
        val SEPARATOR = "-".repeat(45)

        // One record after the separator: four lines of fields, then the SQL
        val RECORD = Regex("Date: (.+)\nDuration: (\\d+)ms\nRows: (\\d+)\nKind: (\\w+)\n(.*)\n", RegexOption.DOT_MATCHES_ALL)

        val DATE_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    }
}
