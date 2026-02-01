package kolbasa.producer

/**
 * Different options how to deal with messages with the same unique keys
 */
enum class DeduplicationMode {
    /**
     * Just throw an exception if there is a message with the same unique key in the queue table.
     * If you send a bunch of messages, all messages (or one batch) will not be inserted (depending on [PartialInsert] option)
     */
    FAIL_ON_DUPLICATE,

    /**
     * Messages with the same unique keys will be silently ignored. No errors will be thrown.
     */
    IGNORE_DUPLICATE
}
