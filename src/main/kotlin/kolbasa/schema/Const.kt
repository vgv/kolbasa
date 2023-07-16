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

    const val REMAINING_ATTEMPTS_COLUMN_NAME = "remaining_attempts"

    const val DATA_COLUMN_NAME = "data"

    const val QUEUE_META_COLUMN_NAME_PREFIX = "meta_"
    const val META_FIELD_NAME_MAX_LENGTH = PG_MAX_IDENTIFIER_LENGTH - QUEUE_META_COLUMN_NAME_PREFIX.length

    const val META_FIELD_NAME_ALLOWED_SYMBOLS = QUEUE_NAME_ALLOWED_SYMBOLS

    /**
     * Max possible size for one string meta field
     * I don't think it's a good idea to have a huge storage (longer than 8Kb) for meta values
     */
    const val META_FIELD_STRING_TYPE_MAX_LENGTH = 8 * 1024 // 8 KB chars

    /**
     * One char in PostgreSQL is enough to store one Java char
     */
    const val META_FIELD_CHAR_TYPE_MAX_LENGTH = 1 // 1 char

    /**
     * Do you really have an enum value longer than 1024 chars?
     */
    const val META_FIELD_ENUM_TYPE_MAX_LENGTH = 1024 // 1024 chars

    /**
     * How many rows to delete at each cleanup iteration
     */
    const val MIN_CLEANUP_ROWS = 100
    const val DEFAULT_CLEANUP_ROWS = 1_000
    const val MAX_CLEANUP_ROWS = 100_000

    /**
     * How many iterations at each cleanup
     */
    const val MIN_CLEANUP_ITERATIONS = 1
    const val DEFAULT_CLEANUP_ITERATIONS = 10
    const val MAX_CLEANUP_ITERATIONS = 100

    /**
     * Probability to trigger a cleanup
     */
    const val MIN_CLEANUP_PROBABILITY_PERCENT = 0
    const val DEFAULT_CLEANUP_PROBABILITY_PERCENT = 20
    const val MAX_CLEANUP_PROBABILITY_PERCENT = 100
}
