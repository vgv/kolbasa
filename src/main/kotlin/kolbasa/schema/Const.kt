package kolbasa.schema

internal object Const {
    /**
     * PG default identifier length
     * https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
     */
    private const val PG_MAX_IDENTIFIER_LENGTH = 63

    const val QUEUE_TABLE_NAME_PREFIX = "q_"
    const val QUEUE_NAME_MAX_LENGTH = PG_MAX_IDENTIFIER_LENGTH - QUEUE_TABLE_NAME_PREFIX.length
    const val QUEUE_NAME_ALLOWED_SYMBOLS = "0123456789" +
        "abcdefghijklmnopqrstuvwxyz" +
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "_"

    const val ID_COLUMN_NAME = "id"

    const val CREATED_AT_COLUMN_NAME = "created_at"
    const val SCHEDULED_AT_COLUMN_NAME = "scheduled_at"
    const val PROCESSING_AT_COLUMN_NAME = "processing_at"

    const val PRODUCER_COLUMN_NAME = "producer"
    const val CONSUMER_COLUMN_NAME = "consumer"
    const val PRODUCER_CONSUMER_VALUE_LENGTH = 256

    const val ATTEMPTS_COLUMN_NAME = "attempts"

    const val DATA_COLUMN_NAME = "data"

    const val QUEUE_META_COLUMN_NAME_PREFIX = "meta_"
    const val META_FIELD_NAME_MAX_LENGTH = PG_MAX_IDENTIFIER_LENGTH - QUEUE_META_COLUMN_NAME_PREFIX.length

    const val META_FIELD_NAME_ALLOWED_SYMBOLS = QUEUE_NAME_ALLOWED_SYMBOLS
}
