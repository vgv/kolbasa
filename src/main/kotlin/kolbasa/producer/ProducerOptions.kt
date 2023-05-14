package kolbasa.producer

import kolbasa.queue.Checks

data class ProducerOptions(
    val producer: String? = null,
    val batchSize: Int = DEFAULT_BATCH_SIZE,
    val partialInsert: PartialInsert = PartialInsert.PROHIBITED
) {

    init {
        Checks.checkProducerName(producer)
        Checks.checkProducerBatchSize(batchSize)
    }

    private companion object {
        private const val DEFAULT_BATCH_SIZE = 500
    }
}

enum class PartialInsert {
    PROHIBITED,
    UNTIL_FIRST_FAILURE,
    INSERT_AS_MANY_AS_POSSIBLE
}
