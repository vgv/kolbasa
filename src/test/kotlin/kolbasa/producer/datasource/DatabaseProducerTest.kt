package kolbasa.producer.datasource

import kolbasa.AbstractPostgresqlTest
import kolbasa.pg.DatabaseExtensions.readInt
import kolbasa.producer.MessageOptions
import kolbasa.producer.ProducerOptions
import kolbasa.producer.SendMessage
import kolbasa.producer.SendResult.Companion.onlyDuplicated
import kolbasa.producer.SendResult.Companion.onlyFailed
import kolbasa.producer.SendResult.Companion.onlySuccessful
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.meta.FieldOption
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaValues
import kolbasa.queue.meta.Metadata
import kolbasa.schema.Const
import kolbasa.schema.IdRange
import kolbasa.schema.SchemaHelpers
import java.time.Duration
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class DatabaseProducerTest : AbstractPostgresqlTest() {

    private val FIELD = MetaField.int("field", FieldOption.STRICT_UNIQUE)

    private val queue = Queue.of(
        "local",
        PredefinedDataTypes.String,
        metadata = Metadata.of(FIELD)
    )

    @BeforeTest
    fun before() {
        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
    }

    @Test
    fun testSendSimpleData() {
        val producer = DatabaseProducer(dataSource)

        val id1 = producer.send(queue, "bugaga").let { result ->
            assertEquals(0, result.failedMessages)
            assertEquals(0, result.onlyFailed().size)
            assertEquals(0, result.onlyDuplicated().size)
            assertEquals(1, result.onlySuccessful().size) // only one successful result
            result.onlySuccessful().first().id
        }

        val id2 = producer.send(queue, "bugaga").let { result ->
            assertEquals(0, result.failedMessages)
            assertEquals(0, result.onlyFailed().size)
            assertEquals(0, result.onlyDuplicated().size)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }

        assertEquals(IdRange.LOCAL_RANGE.min, id1.localId)
        assertEquals(IdRange.LOCAL_RANGE.min + 1, id2.localId)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendSimpleData_WithCustomProducerName() {
        val firstProducerName = "first_producer"
        val secondProducerName = "second_producer"
        val firstProducer = DatabaseProducer(dataSource, ProducerOptions(producer = firstProducerName))
        val secondProducer = DatabaseProducer(dataSource, ProducerOptions(producer = secondProducerName))

        val id1 = firstProducer.send(queue, "bugaga").let { result ->
            assertEquals(0, result.failedMessages)
            assertEquals(0, result.onlyFailed().size)
            assertEquals(0, result.onlyDuplicated().size)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }

        val id2 = secondProducer.send(queue, "bugaga").let { result ->
            assertEquals(0, result.failedMessages)
            assertEquals(0, result.onlyFailed().size)
            assertEquals(0, result.onlyDuplicated().size)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }

        assertEquals(IdRange.LOCAL_RANGE.min, id1.localId)
        assertEquals(IdRange.LOCAL_RANGE.min + 1, id2.localId)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
        // check first producer
        assertEquals(
            1,
            dataSource.readInt("select count(*) from ${queue.dbTableName} where ${Const.PRODUCER_COLUMN_NAME}='$firstProducerName'")
        )
        assertEquals(
            1,
            dataSource.readInt("select count(*) from ${queue.dbTableName} where ${Const.PRODUCER_COLUMN_NAME}='$secondProducerName'")
        )
    }

    @Test
    fun testSendSimpleData_AsSendMessage() {
        val producer = DatabaseProducer(dataSource)

        val id1 = producer.send(queue, SendMessage("bugaga", MetaValues.of(FIELD.value(1)))).let { (failedMessages, result) ->
            assertEquals(0, failedMessages)
            assertEquals(0, result.onlyFailed().size)
            assertEquals(0, result.onlyDuplicated().size)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }

        val id2 = producer.send(queue, SendMessage("bugaga", MetaValues.of(FIELD.value(2)))).let { (failedMessages, result) ->
            assertEquals(0, failedMessages)
            assertEquals(0, result.onlyFailed().size)
            assertEquals(0, result.onlyDuplicated().size)
            assertEquals(1, result.onlySuccessful().size)
            result.onlySuccessful().first().id
        }

        assertEquals(IdRange.LOCAL_RANGE.min, id1.localId)
        assertEquals(IdRange.LOCAL_RANGE.min + 1, id2.localId)

        // check database
        assertEquals(2, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

    @Test
    fun testSendSimpleData_CheckDurationBiggerThanMaxInt() {
        val producer = DatabaseProducer(dataSource)
        val delay = Duration.ofHours(24 * 365 * 1000) // approx. 1000 years

        val result = producer.send(
            queue,
            SendMessage("bugaga", messageOptions = MessageOptions(delay = delay))
        )
        assertEquals(0, result.failedMessages, "Result: $result")
        assertEquals(1, result.onlySuccessful().size)

        // check database
        // TODO: check created_at and scheduled_at and compare it with 1000 years duration above
        assertEquals(1, dataSource.readInt("select count(*) from ${queue.dbTableName}"))
    }

}
