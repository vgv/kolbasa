package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.ReceiveOptions
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.Metadata
import kolbasa.utils.JdbcHelpers.readInt
import kolbasa.utils.JdbcHelpers.readLongOrNull
import kolbasa.utils.JdbcHelpers.useStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration

class SqlPutFunctionTest : AbstractPostgresqlTest() {

    // Meta fields. NB: the generated function param for a meta field is named after its *column*
    // (meta_<snake_case_name>), not the bare field name — see QueueHelpers.generateMetaColumnDbName.
    private val priority = MetaField.int("priority", FieldOption.SEARCH)
    private val dedup = MetaField.string("dedup", FieldOption.UNTOUCHED_UNIQUE)

    private fun queue(name: String, delay: Duration = Duration.ZERO) =
        Queue.builder(name, PredefinedDataTypes.String)
            .metadata(Metadata.of(priority, dedup))
            .options(
                QueueOptions.builder()
                    .defaultDelay(delay)
                    .enableSqlPutFunction()
                    .build()
            )
            .build()

    private fun functionCount(name: String): Int = dataSource.readInt(
        "select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace " +
            "where n.nspname = current_schema and p.proname = '$name'"
    )

    // Calls the put function and returns the generated id, or null when the function returned NULL
    // (an ignored duplicate). A real id can legitimately be 0 (bucket 0's identity range starts at 0),
    // which is exactly why we need a NULL-aware reader rather than a numeric check.
    private fun put(call: String): Long? = dataSource.readLongOrNull("select $call")

    @Test
    fun testPutThenConsume() {
        val queue = queue("orders")
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        assertNotNull(put("q_orders_put(data => 'hello', meta_priority => 7)"))

        val msg = requireNotNull(
            DatabaseConsumer(dataSource).receive(queue, ReceiveOptions(readMetadata = true))
        )

        assertEquals("hello", msg.data)
        assertEquals(7, msg.meta.get(priority))
        assertNull(msg.meta.getOrNull(dedup))
    }

    @Test
    fun testDefaultsAndExplicitDelay() {
        val queue = queue("delayed", delay = Duration.ofMinutes(5))
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // Uses the baked 5-minute default delay => SCHEDULED, not yet visible.
        assertNotNull(put("q_delayed_put(data => 'later')"))
        assertNull(DatabaseConsumer(dataSource).receive(queue), "should be delayed")

        // Explicit zero delay => immediately visible, with a meta value passed alongside the delay param.
        assertNotNull(put("q_delayed_put(data => 'now', delay => interval '0', meta_priority => 3)"))
        val msg = requireNotNull(
            DatabaseConsumer(dataSource).receive(queue, ReceiveOptions(readMetadata = true))
        )
        assertEquals("now", msg.data)
        assertEquals(3, msg.meta.get(priority))
        assertNull(msg.meta.getOrNull(dedup))
    }

    @Test
    fun testIgnoreDuplicates() {
        val q = queue("dedup_queue")
        SchemaHelpers.createOrUpdateQueues(dataSource, q)

        // First insert succeeds and returns a real id.
        assertNotNull(put("q_dedup_queue_put(data => 'a', meta_dedup => 'k1', ignore_duplicates => true)"))
        // The duplicate is skipped: the function returns NULL.
        assertNull(put("q_dedup_queue_put(data => 'b', meta_dedup => 'k1', ignore_duplicates => true)"), "dup => null")
        assertEquals(1, dataSource.readInt("select count(*) from q_dedup_queue"))

        // Without ignore_duplicates the unique violation propagates.
        assertThrows(Exception::class.java) {
            put("q_dedup_queue_put(data => 'c', meta_dedup => 'k1')")
        }

        // The surviving message carries the dedup meta written from SQL; the unset field is null.
        val msg = requireNotNull(DatabaseConsumer(dataSource).receive(q, ReceiveOptions(readMetadata = true)))
        assertEquals("a", msg.data)
        assertEquals("k1", msg.meta.get(dedup))
        assertNull(msg.meta.getOrNull(priority))
    }

    @Test
    fun testIdempotentSecondRun() {
        val queue = queue("idempotent")
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)

        // The Table carries its put function (extractRawSchema always populates it). A second pass with the
        // same config must hash-match the stored COMMENT and emit nothing.
        val existingTable = SchemaExtractor.extractRawSchema(dataSource, setOf("q_idempotent"))["q_idempotent"]
        val schema = SchemaGenerator.generateTableSchema(queue, existingTable, IdRange.generateRange(Node.MIN_BUCKET))
        assertTrue(schema.isEmpty, "second run must be a no-op, got: $schema")
    }

    @Test
    fun testMetadataChangeReplacesWithoutOrphanOverload() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue("evolve"))
        assertEquals(1, functionCount("q_evolve_put"))

        // Add a meta field => signature changes => function regenerated, still exactly one overload
        // (bare-name DROP guarantees a single function with that name).
        val accountId = MetaField.long("account_id", FieldOption.SEARCH)
        val evolved = Queue.builder("evolve", PredefinedDataTypes.String)
            .metadata(Metadata.of(priority, dedup, accountId))
            .options(QueueOptions.builder().enableSqlPutFunction().build())
            .build()
        SchemaHelpers.createOrUpdateQueues(dataSource, evolved)

        assertEquals(1, functionCount("q_evolve_put"))
        assertNotNull(put("q_evolve_put(data => 'x', meta_account_id => 42)")) // new param exists

        // The newly added meta field round-trips through the consumer; the older fields stay null.
        val msg = requireNotNull(DatabaseConsumer(dataSource).receive(evolved, ReceiveOptions(readMetadata = true)))
        assertEquals("x", msg.data)
        assertEquals(42L, msg.meta.get(accountId))
        assertNull(msg.meta.getOrNull(priority))
        assertNull(msg.meta.getOrNull(dedup))
    }

    @Test
    fun testTrigger() {
        val q = queue("events")
        SchemaHelpers.createOrUpdateQueues(dataSource, q)

        // Create test table and trigger
        dataSource.useStatement { st ->
            st.execute("create table biz(id serial primary key, payload text)")

            // The trigger forwards the inserted row into the queue
            st.execute(
                """
                create function biz_enqueue() returns trigger language plpgsql as '
                    begin
                        perform q_events_put(data => new.payload, meta_priority => new.id);
                        return new;
                    end
                '
                """.trimIndent()
            )

            st.execute("create trigger biz_trg after insert on biz for each row execute procedure biz_enqueue()")
        }
        // Insert a few rows
        dataSource.useStatement { st -> st.execute("insert into biz(payload) values ('first')") }
        dataSource.useStatement { st -> st.execute("insert into biz(payload) values ('second')") }
        dataSource.useStatement { st -> st.execute("insert into biz(payload) values ('third')") }

        val messages = DatabaseConsumer(dataSource).receive(q, limit = 100, ReceiveOptions(readMetadata = true))
        assertEquals(3, messages.size)

        // First
        assertEquals("first", messages[0].data)
        assertEquals(1, messages[0].meta.get(priority)) // forwarded from biz.id (serial starts at 1)
        assertNull(messages[0].meta.getOrNull(dedup))

        // Second
        assertEquals("second", messages[1].data)
        assertEquals(2, messages[1].meta.get(priority))
        assertNull(messages[1].meta.getOrNull(dedup))

        // Third
        assertEquals("third", messages[2].data)
        assertEquals(3, messages[2].meta.get(priority)) // forwarded from biz.id (serial starts at 1)
        assertNull(messages[2].meta.getOrNull(dedup))
    }
}
