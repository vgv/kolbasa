package kolbasa.schema

import kolbasa.AbstractPostgresqlTest
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.schema.SchemaGeneratorTest.TestMeta
import java.time.Duration
import kotlin.test.Test

class SchemaHelpersTest: AbstractPostgresqlTest() {

    @Test
    fun testGenerateSchema_CheckDurationBiggerThanMaxInt() {
        val duration = Duration.ofHours(24 * 365 * 1000) // approx. 1000 years

        val queue = Queue.builder("big_delay", PredefinedDataTypes.String)
            .metadata(TestMeta::class.java)
            .options(QueueOptions(defaultDelay = duration, defaultVisibilityTimeout = duration))
            .build()

        SchemaHelpers.updateDatabaseSchema(dataSource, queue)
    }


}
