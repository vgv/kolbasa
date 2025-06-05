package kolbasa.mutator.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.mutator.AddRemainingAttempts
import kolbasa.mutator.MutatorOptions
import kolbasa.pg.DatabaseExtensions.readIntList
import kolbasa.producer.MessageOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.datasource.DatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.Searchable
import kolbasa.schema.Const
import kolbasa.schema.SchemaHelpers
import kotlin.test.*

class DatabaseMutatorTest : AbstractPostgresqlTest() {

    internal data class TestMeta(@Searchable val field: Int)

    private val queue = Queue.of(
        "local",
        PredefinedDataTypes.String,
        metadata = TestMeta::class.java
    )

    @BeforeTest
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
    }

    @Test
    fun testSimpleMutate_IdList() {
        val attempts = 123
        val attemptsDelta = 456

        val producer = DatabaseProducer(dataSource)
        val data1 = (1..50).map {
            val meta = TestMeta(it)
            SendMessage(it.toString(), meta, MessageOptions(attempts = attempts))
        }
        val data2 = (100..150).map {
            val meta = TestMeta(it)
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
        val attempts = 123
        val attemptsDelta = 456

        val producer = DatabaseProducer(dataSource)
        val data = (1..100).map {
            val meta = TestMeta(it)
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
            (TestMeta::field lessEq 67) or (TestMeta::field eq 99)
        }

        val expectedMutatedIds = sentMessages
            .filter {
                val meta = requireNotNull(it.message.meta)
                // Same expression as above
                meta.field <= 67 || meta.field == 99
            }
            .map { it.id }


        assertFalse(mutateResult.truncated)
        assertEquals(expectedMutatedIds.size, mutateResult.mutatedMessages)
        assertEquals(expectedMutatedIds, mutateResult.onlyMutated().map { it.id })
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
        val maxMutated = 20
        val attempts = 123
        val attemptsDelta = 456

        val producer = DatabaseProducer(dataSource)
        val data = (1..100).map {
            val meta = TestMeta(it)
            SendMessage(it.toString(), meta, MessageOptions(attempts = attempts))
        }

        val sentMessages = producer.send(queue, data).onlySuccessful()

        // Direct database check
        dataSource.readIntList("select distinct ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} from ${queue.dbTableName}").also { list ->
            assertEquals(1, list.size)
            assertEquals(attempts, list.first())
        }

        // Ok, mutate some messages (by filter) and then check
        val mutatorOptions = MutatorOptions(maxMutatedRowsKeepInMemory = maxMutated)
        val mutator = DatabaseMutator(dataSource, mutatorOptions)
        val mutations = listOf(AddRemainingAttempts(attemptsDelta))
        val mutateResult = mutator.mutate(queue, mutations) {
            (TestMeta::field lessEq 67) or (TestMeta::field eq 99)
        }

        val expectedMutatedIds = sentMessages
            .filter {
                val meta = requireNotNull(it.message.meta)
                // Same expression as above
                meta.field <= 67 || meta.field == 99
            }
            .map { it.id }


        assertTrue(mutateResult.truncated)
        assertEquals(expectedMutatedIds.size, mutateResult.mutatedMessages)
        assertEquals(expectedMutatedIds.take(maxMutated), mutateResult.onlyMutated().map { it.id })
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

}
