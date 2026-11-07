package kolbasa.telemetry.sql

import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.EnumSet

class SqlDumpConfigTest {

    private val first = Queue.of("first", PredefinedDataTypes.String)
    private val second = Queue.of("second", PredefinedDataTypes.String)

    @Test
    fun testQueueWithoutKinds() {
        val config = SqlDumpConfig.builder().queue(first).build()

        // No kinds means every kind
        assertEquals(mapOf("first" to EnumSet.allOf(StatementKind::class.java)), config.queues)
    }

    @Test
    fun testQueueAgain() {
        val config = SqlDumpConfig.builder()
            .queue(first, StatementKind.SWEEP)
            .queue(first, StatementKind.MUTATE_BY_ID)
            .build()

        // The second call replaces the kinds of the queue
        assertEquals(mapOf("first" to EnumSet.of(StatementKind.MUTATE_BY_ID)), config.queues)
    }

    @Test
    fun testBuildCopies() {
        val builder = SqlDumpConfig.builder().queue(first, StatementKind.SWEEP)
        val config = builder.build()

        // Later calls to the builder don't change the config built before
        builder.queue(first, StatementKind.MUTATE_BY_ID)
        builder.queue(second, StatementKind.SWEEP)
        assertEquals(mapOf("first" to EnumSet.of(StatementKind.SWEEP)), config.queues)
    }
}
