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

    const val META_FIELD_NAME_PREFIX = "meta_"
    const val META_FIELD_NAME_MAX_LENGTH = PG_MAX_IDENTIFIER_LENGTH - META_FIELD_NAME_PREFIX.length

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
     * How many rows to delete at each iteration during sweep
     */
    const val MIN_SWEEP_ROWS = 100
    const val DEFAULT_SWEEP_ROWS = 1_000
    const val MAX_SWEEP_ROWS = 100_000

    /**
     * How many iterations at each sweep
     */
    const val MIN_SWEEP_ITERATIONS = 1
    const val DEFAULT_SWEEP_ITERATIONS = 5
    const val MAX_SWEEP_ITERATIONS = 100

    /**
     * How often we want to trigger a sweep?
     * Every fifth consume? Every tenth? Every hundredth?
     *
     * Default value is 5, so, it means that every fifth consume will trigger a sweep.
     * If you want to trigger a sweep at every consume, you have to use period = 1 (EVERYTIME_SWEEP_PERIOD)
     */
    const val EVERYTIME_SWEEP_PERIOD = 1
    const val MIN_SWEEP_PERIOD = EVERYTIME_SWEEP_PERIOD
    const val DEFAULT_SWEEP_PERIOD = 5
    const val MAX_SWEEP_PERIOD = 1_000_000_000
}
