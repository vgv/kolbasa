package kolbasa.mutator.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.consumer.datasource.DatabaseConsumer
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.AddScheduledAt
import kolbasa.mutator.MutatorOptions
import kolbasa.mutator.SetScheduledAt
import kolbasa.utils.JdbcHelpers.readIntList
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.Const
import kolbasa.schema.SchemaHelpers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class DatabaseMutatorTest : AbstractPostgresqlTest() {

    private val FIELD = MetaField.int("field", FieldOption.SEARCH)

    private val queue = Queue.of(
        "local",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIELD)
    )

    @BeforeEach
    fun before() {
        SchemaHelpers.createOrUpdateQueues(dataSource, queue)
    }

    @Test
    fun testSimpleMutate_IdList() {
        val attempts = 123
        val attemptsDelta = 456

        val producer = DatabaseProducer(dataSource)
        val data1 = (1..50).map {
            val meta = MetaValues.of(FIELD.value(it))
            SendMessage(it.toString(), meta, MessageOptions(attempts = attempts))
        }
        val data2 = (100..150).map {
            val meta = MetaValues.of(FIELD.value(it))
            SendMessage(it.toString(), meta, MessageOptions(attempts = attempts))
        }

        // First list, to mutate
        val firstIds = producer.send(queue, data1).onlySuccessful().map { it.id }
        // Second list, to check (not mutate)
        val secondIds = producer.send(queue, data2).onlySuccessful().map { it.id }

        // Direct database check
        dataSource.readIntList("select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} from ${queue.dbTableName}").also { list ->
            assertEquals(1, list.size)
            assertEquals(attempts, list.first())
        }

        // Ok, mutate firstIds list and then check
        val mutator = DatabaseMutator(dataSource)
        val mutations = listOf(AddRemainingAttempts(attemptsDelta))
        val mutateResult = mutator.mutate(queue, mutations, firstIds)

        assertFalse(mutateResult.truncated)
        assertEquals(data1.size, mutateResult.mutatedMessages)
        assertEquals(firstIds, mutateResult.onlyMutated().map { it.id })
        val uniqueNewAttempts = mutateResult.onlyMutated().map { it.remainingAttempts }.distinct()
        assertEquals(listOf(attempts + attemptsDelta), uniqueNewAttempts)

        // Second direct database check
        // let's check firstIds list with changed remainingAttempts field
        run {
            val idList = firstIds.joinToString(separator = ",", prefix = "(", postfix = ")") { id ->
                "(${id.localId}, ${id.shard})"
            }
            val query = """
                select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
                from ${queue.dbTableName}
                where (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in $idList
            """

            val list = dataSource.readIntList(query)
            assertEquals(1, list.size)
            // changed attempts, old+delta
            assertEquals(attempts + attemptsDelta, list.first())
        }

        // let's check secondIds list with no changes
        run {
            val idList = secondIds.joinToString(separator = ",", prefix = "(", postfix = ")") { id ->
                "(${id.localId}, ${id.shard})"
            }
            val query = """
                select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
                from ${queue.dbTableName}
                where (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in $idList
            """

            val list = dataSource.readIntList(query)
            assertEquals(1, list.size)
            // old attempts, no changes
            assertEquals(attempts, list.first())
        }
    }

    @Test
    fun testSimpleMutate_Filter() {
        // Make database dirty
        createGarbage()

        val attempts = 123
        val attemptsDelta = 456

        val producer = DatabaseProducer(dataSource)
        val data = (1..100).map {
            val meta = MetaValues.of(FIELD.value(it))
            SendMessage(it.toString(), meta, MessageOptions(attempts = attempts))
        }

        val sentMessages = producer.send(queue, data).onlySuccessful()

        // Direct database check
        dataSource.readIntList("select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} from ${queue.dbTableName}").also { list ->
            assertEquals(1, list.size)
            assertEquals(attempts, list.first())
        }

        // Ok, mutate some messages (by filter) and then check
        val mutator = DatabaseMutator(dataSource)
        val mutations = listOf(AddRemainingAttempts(attemptsDelta))
        val mutateResult = mutator.mutate(queue, mutations) {
            (FIELD lessEq 67) or (FIELD eq 99)
        }

        val expectedMutatedIds = sentMessages
            .filter {
                val meta = requireNotNull(it.message.meta)
                // Same expression as above
                meta.get(FIELD) <= 67 || meta.get(FIELD) == 99
            }
            .map { it.id }


        assertFalse(mutateResult.truncated)
        assertEquals(expectedMutatedIds.size, mutateResult.mutatedMessages)
        assertEquals(expectedMutatedIds.toSet(), mutateResult.onlyMutated().map { it.id }.toSet())
        val uniqueNewAttempts = mutateResult.onlyMutated().map { it.remainingAttempts }.distinct()
        assertEquals(listOf(attempts + attemptsDelta), uniqueNewAttempts)

        // Second direct database check
        // let's check expectedMutatedIds list with changed remainingAttempts field
        run {
            val idList = expectedMutatedIds.joinToString(separator = ",", prefix = "(", postfix = ")") { id ->
                "(${id.localId}, ${id.shard})"
            }
            val query = """
                select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
                from ${queue.dbTableName}
                where (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in $idList
            """

            val list = dataSource.readIntList(query)
            assertEquals(1, list.size)
            // changed attempts, old+delta
            assertEquals(attempts + attemptsDelta, list.first())
        }

        // let's check everything except expectedMutatedIds list
        // no changes expected
        run {
            val idList = expectedMutatedIds.joinToString(separator = ",", prefix = "(", postfix = ")") { id ->
                "(${id.localId}, ${id.shard})"
            }
            val query = """
                select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
                from ${queue.dbTableName}
                where (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) not in $idList
            """

            val list = dataSource.readIntList(query)
            assertEquals(1, list.size)
            // old attempts, no changes
            assertEquals(attempts, list.first())
        }
    }

    @Test
    fun testSimpleMutate_Filter_TruncatedResponse() {
        // Make database dirty
        createGarbage()

        val maxMutated = 20
        val attempts = 123
        val attemptsDelta = 456

        val producer = DatabaseProducer(dataSource)
        val data = (1..100).map {
            val meta = MetaValues.of(FIELD.value(it))
            SendMessage(it.toString(), meta, MessageOptions(attempts = attempts))
        }

        val sentMessages = producer.send(queue, data).onlySuccessful()

        // Direct database check
        dataSource.readIntList("select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} from ${queue.dbTableName}").also { list ->
            // Check all messages have the same attempts
            assertEquals(1, list.size)
            assertEquals(attempts, list.first())
        }

        // Ok, mutate some messages (by filter) and then check
        val mutatorOptions = MutatorOptions(maxMutatedMessagesKeepInMemory = maxMutated)
        val mutator = DatabaseMutator(dataSource, mutatorOptions)
        val mutations = listOf(AddRemainingAttempts(attemptsDelta))
        val mutateResult = mutator.mutate(queue, mutations) {
            (FIELD lessEq 67) or (FIELD eq 99)
        }

        val expectedMutatedIds = sentMessages
            .filter {
                val meta = requireNotNull(it.message.meta)
                // Same expression as above
                meta.get(FIELD) <= 67 || meta.get(FIELD) == 99
            }
            .map { it.id }

        assertTrue(mutateResult.truncated)
        assertEquals(expectedMutatedIds.size, mutateResult.mutatedMessages)
        // Since response is truncated, we can check only that all returned ids are from expectedMutatedIds list
        assertTrue(expectedMutatedIds.containsAll(mutateResult.onlyMutated().map { it.id })) {
            "expected: $expectedMutatedIds, mutated: $mutateResult"
        }

        val uniqueNewAttempts = mutateResult.onlyMutated().map { it.remainingAttempts }.distinct()
        assertEquals(listOf(attempts + attemptsDelta), uniqueNewAttempts)

        // Second direct database check
        // let's check expectedMutatedIds list with changed remainingAttempts field
        run {
            val idList = expectedMutatedIds.joinToString(separator = ",", prefix = "(", postfix = ")") { id ->
                "(${id.localId}, ${id.shard})"
            }
            val query = """
                select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
                from ${queue.dbTableName}
                where (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) in $idList
            """

            val list = dataSource.readIntList(query)
            assertEquals(1, list.size)
            // changed attempts, old+delta
            assertEquals(attempts + attemptsDelta, list.first())
        }

        // let's check everything except expectedMutatedIds list
        // no changes expected
        run {
            val idList = expectedMutatedIds.joinToString(separator = ",", prefix = "(", postfix = ")") { id ->
                "(${id.localId}, ${id.shard})"
            }
            val query = """
                select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
                from ${queue.dbTableName}
                where (${Const.ID_COLUMN_NAME}, ${Const.SHARD_COLUMN_NAME}) not in $idList
            """

            val list = dataSource.readIntList(query)
            assertEquals(1, list.size)
            // old attempts, no changes
            assertEquals(attempts, list.first())
        }
    }

    @Test
    fun testMutate_CheckDurationBiggerThanMaxInt() {
        createGarbage()

        val duration = Duration.ofHours(24 * 365 * 1000) // approx. 1000 years

        val producer = DatabaseProducer(dataSource)
        val data = (1..100).map {
            SendMessage(it.toString())
        }

        // Id list to mutate
        val ids = producer.send(queue, data).onlySuccessful().map { it.id }

        // Ok, mutate messages and then check
        run {
            val mutator = DatabaseMutator(dataSource)
            val mutations = listOf(AddScheduledAt(delta = duration))
            val mutateResult = mutator.mutate(queue, mutations, ids)

            assertFalse(mutateResult.truncated)
            assertEquals(data.size, mutateResult.mutatedMessages)
            assertEquals(ids, mutateResult.onlyMutated().map { it.id })
        }

        run {
            val mutator = DatabaseMutator(dataSource)
            val mutations = listOf(SetScheduledAt(newValue = duration))
            val mutateResult = mutator.mutate(queue, mutations, ids)

            assertFalse(mutateResult.truncated)
            assertEquals(data.size, mutateResult.mutatedMessages)
            assertEquals(ids, mutateResult.onlyMutated().map { it.id })
        }
    }

    private fun createGarbage() {
        // We have to insert and delete a lot of messages to make sure that table is fragmented enough for testing
        // Numbers here are just experimental, but they work to generate "enough" garbage
        val producer = DatabaseProducer(dataSource)
        val consumer = DatabaseConsumer(dataSource)

        (1..100).forEach { _ ->
            val data = (1..1000).map {
                SendMessage("${System.nanoTime()}_${it}", meta = MetaValues.of(FIELD.value(it)))
            }

            val ids = producer.send(queue, data).onlySuccessful().map { it.id }

            consumer.delete(queue, ids)
        }
    }

}
