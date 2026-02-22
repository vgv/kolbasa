package kolbasa.producer

/**
 * Controls how the producer handles messages with duplicate unique keys.
 *
 * Unique keys are defined by meta fields with uniqueness constraints such as
 * [FieldOption.ALL_LIVE_UNIQUE][kolbasa.queue.meta.FieldOption.ALL_LIVE_UNIQUE] or
 * [FieldOption.UNTOUCHED_UNIQUE][kolbasa.queue.meta.FieldOption.UNTOUCHED_UNIQUE]. When you attempt to send
 * a message whose unique key already exists in the queue table, this mode determines the behavior.
 *
 * ## Options Hierarchy
 *
 * The deduplication mode can be configured at two levels:
 * 1. [ProducerOptions.deduplicationMode] — applies to all `send()` calls from this producer
 * 2. [SendOptions.deduplicationMode] — overrides producer setting for a specific `send()` call
 *
 * The default value is [FAIL_ON_DUPLICATE].
 *
 * ## Usage Example
 *
 * ```kotlin
 * // Define a queue with a unique meta field
 * val USER_ID = MetaField.int("user_id", FieldOption.ALL_LIVE_UNIQUE)
 * val queue = Queue.of("notifications", PredefinedDataTypes.String, metadata = Metadata.of(USER_ID))
 *
 * // Option 1: Set at producer level
 * val producer = DatabaseProducer(
 *     dataSource,
 *     ProducerOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATE)
 * )
 *
 * // Option 2: Set per send() call
 * producer.send(queue, SendRequest(
 *     data = messages,
 *     sendOptions = SendOptions(deduplicationMode = DeduplicationMode.IGNORE_DUPLICATE)
 * ))
 * ```
 *
 * @see ProducerOptions.deduplicationMode
 * @see SendOptions.deduplicationMode
 * @see kolbasa.queue.meta.FieldOption.ALL_LIVE_UNIQUE
 * @see kolbasa.queue.meta.FieldOption.UNTOUCHED_UNIQUE
 * @see PartialInsert
 */
enum class DeduplicationMode {
    /**
     * Fail the batch when a duplicate unique key is detected.
     *
     * When a message with a duplicate unique key is encountered, the entire batch (or all messages,
     * depending on [PartialInsert] configuration) will fail to insert. The duplicate message and
     * potentially other messages in the same batch will be reported in [SendResult.onlyFailed].
     *
     * This is the default mode and is useful when duplicates indicate a bug or data integrity issue
     * that should be investigated.
     *
     * ## SendResult Behavior
     *
     * When duplicates are detected:
     * - [SendResult.onlyFailed] contains messages that failed (including the duplicate)
     * - [SendResult.onlyDuplicated] will be empty (duplicates are treated as failures)
     * - [SendResult.failedMessages] reflects the count of messages that were not inserted
     *
     * ## Example
     *
     * ```kotlin
     * // Sending 100 messages where one has a duplicate key
     * val result = producer.send(queue, messagesWithOneDuplicate)
     *
     * result.onlySuccessful().size  // 0 - entire batch failed
     * result.onlyDuplicated().size  // 0 - duplicates reported as failures
     * result.onlyFailed().size      // 1 - one failure containing all 100 messages
     * result.failedMessages         // 100 - all messages failed
     * ```
     *
     * @see PartialInsert for controlling batch failure granularity
     */
    FAIL_ON_DUPLICATE,
    @Deprecated("Use FAIL_ON_DUPLICATE instead", ReplaceWith("FAIL_ON_DUPLICATE"))
    ERROR,

    /**
     * Silently skip messages with duplicate unique keys.
     *
     * When a message with a duplicate unique key is encountered, that specific message is skipped
     * and reported in [SendResult.onlyDuplicated], while all other messages are inserted normally.
     * No errors are raised for duplicates.
     *
     * This mode is useful for idempotent operations where re-sending the same message should be
     * a no-op, or when you want to insert only new messages from a batch that may contain duplicates.
     *
     * ## SendResult Behavior
     *
     * When duplicates are detected:
     * - [SendResult.onlySuccessful] contains messages that were inserted
     * - [SendResult.onlyDuplicated] contains messages that were skipped due to duplicate keys
     * - [SendResult.onlyFailed] will be empty (duplicates are not treated as failures)
     * - [SendResult.failedMessages] will be 0
     *
     * ## Example
     *
     * ```kotlin
     * // Sending 100 messages where one has a duplicate key
     * val result = producer.send(queue, messagesWithOneDuplicate)
     *
     * result.onlySuccessful().size  // 99 - non-duplicate messages inserted
     * result.onlyDuplicated().size  // 1 - the duplicate was skipped
     * result.onlyFailed().size      // 0 - no failures
     * result.failedMessages         // 0 - no failed messages
     * ```
     */
    IGNORE_DUPLICATE,
    @Deprecated("Use IGNORE_DUPLICATE instead", ReplaceWith("IGNORE_DUPLICATE"))
    IGNORE_DUPLICATES,
}
