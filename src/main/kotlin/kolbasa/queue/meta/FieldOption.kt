package kolbasa.queue.meta

/**
 * Specifies indexing and uniqueness behavior for a [MetaField].
 *
 * Each meta field attached to a queue can have different storage and indexing characteristics.
 * The field option you choose affects performance, filtering/sorting capabilities, and deduplication behavior.
 *
 * ## Options Overview
 *
 * | Option              | Index    | Unique | Filter/Sort | Use Case                                    |
 * |---------------------|----------|--------|-------------|---------------------------------------------|
 * | [NONE]              | No       | No     | No          | Store-only data, no queries needed          |
 * | [SEARCH]            | Yes      | No     | Yes         | Fields used for filtering or sorting        |
 * | [ALL_LIVE_UNIQUE]   | Unique   | Yes    | Yes         | Deduplication across all live messages      |
 * | [UNTOUCHED_UNIQUE]  | Unique   | Yes    | Yes         | Deduplication only for unprocessed messages |
 *
 * ## Message States
 *
 * To understand uniqueness options, it helps to know the message lifecycle states:
 *
 * ```
 *                                           ┌───> DEAD (remaining attempts is 0)
 *                                           │
 *  SCHEDULED ──> READY ──────> IN_FLIGHT ───────> COMPLETED
 *                         ↑                 │
 *                         └───── RETRY <────┘
 * ```
 *
 * - **SCHEDULED** — message is waiting, not yet visible to consumers (if delay > 0)
 * - **READY** — message is ready for processing, has never been attempted
 * - **IN_FLIGHT** — message is currently being processed
 * - **RETRY** — processing attempt failed; message is available for the next attempt
 * - **DEAD** — all attempts exhausted, message is logically removed (but still physically present until sweep)
 * - **COMPLETED** — successfully processed and removed
 *
 * The uniqueness options differ in which states are considered "live" for deduplication:
 * - [ALL_LIVE_UNIQUE]: `SCHEDULED` + `READY` + `IN_FLIGHT` + `RETRY`
 * - [UNTOUCHED_UNIQUE]: `SCHEDULED` + `READY` only
 *
 * Read more about message states, transitions and deduplication modes in `docs/Message state transitions.md`
 *
 * ## Usage Example
 *
 * ```kotlin
 * // Store-only field (no queries)
 * val justData = MetaField.string("just_data", FieldOption.NONE)
 *
 * // Searchable field for filtering/sorting
 * val priority = MetaField.int("priority", FieldOption.SEARCH)
 *
 * // Unique field for deduplication (all live messages)
 * val accountId = MetaField.long("account_id", FieldOption.ALL_LIVE_UNIQUE)
 *
 * // Unique field for deduplication (untouched messages only)
 * val requestId = MetaField.string("request_id", FieldOption.UNTOUCHED_UNIQUE)
 *
 * // Create queue with meta fields
 * val queue = Queue.of(
 *     name = "notifications",
 *     dataType = PredefinedDataTypes.String,
 *     metadata = Metadata.of(justData, priority, accountId, requestId)
 * )
 * ```
 *
 * ## Performance Considerations
 *
 * - [NONE] is most efficient — no index overhead
 * - [SEARCH], [ALL_LIVE_UNIQUE], and [UNTOUCHED_UNIQUE] create database indexes
 * - More indexes = slower inserts, but necessary for filtering/sorting/deduplication
 * - Choose the minimal option that meets your requirements
 *
 * @see MetaField
 * @see kolbasa.producer.DeduplicationMode for controlling duplicate handling behavior
 * @see kolbasa.producer.ProducerOptions.deduplicationMode
 */
enum class FieldOption {
    /**
     * No special options
     *
     * Use this value if the meta field is used only to store specific data. It doesn't require any constraints
     * (such as uniqueness) and doesn't require filtering or sorting.
     *
     * This is the most efficient option. If the meta field isn't used for searching, sorting, or deduplication control, you
     * should always select this option.
     */
    NONE,

    /**
     * Field is searchable
     *
     * Use this value if the meta field is used to filter or sort queue messages.
     *
     * Implementation details:
     * A regular index is created in the database for each such field. The general rule is that the more indexes, the
     * worse the performance impact, so use this type of field only if the field actually requires filtering or sorting.
     * Indexes are highly optimized in DBMSs, so there's no need to avoid such fields at all costs; the queue design should
     * be reasonable. If your application truly requires a queue with one or more meta fields for filtering, add them;
     * don't build strange and fragile workarounds.
     */
    SEARCH,

    /**
     * Field is unique and searchable
     *
     * Use this value if the meta field is used for record deduplication. In addition to deduplication, this option provides
     * filtering and sorting, similar to the [SEARCH] option.
     *
     * Any message in a queue is in one of six states:
     * 1. `SCHEDULED` - a message has been placed in the queue, but no attempt has been made to process it; it is still waiting
     * and not yet visible to consumers if [QueueOptions.defaultDelay][kolbasa.queue.QueueOptions.defaultDelay] (and other delays)
     * is set to greater than zero. If delay is zero, the message is immediately moved to the next state `READY`.
     * 2. `READY` - a message is ready to be processed, it is visible to consumers and can be received for processing. The
     * message has not been processed before; this will be the first attempt.
     * 3. `IN_FLIGHT` - a message is being processed right now. It has been received for processing by a
     * consumer, but the processing has not yet been completed.
     * 4. `RETRY` - the processing attempt failed (the visibility timeout expired); the message is available
     * for the next attempt. Like `READY` the message is available for processing; unlike `READY`, there was at least one
     * attempt to process the message.
     * 5. `DEAD` - all attempts to process it were unsuccessful; the message is waiting for sweep to clear it from the queue.
     * 6. `COMPLETED` - the message has been successfully processed and removed from the queue.
     *
     * ```
     *                                           ┌───> DEAD (remaining attempts is 0)
     *                                           │
     *  SCHEDULED ──> READY ──────> IN_FLIGHT ───────> COMPLETED
     *                         ↑                 │
     *                         └───── RETRY <────┘
     * ```
     *
     * For example, we have a `user_id` meta field by which we want to dedupe messages in the queue.
     * There is already a message with `user_id = 123` in the queue, and we want to add another one. How should uniqueness
     * control work depending on the state of the first, already existing message?
     *
     * It's clear that if the first message is `DEAD`, the second one should be added to the queue. A dead message is still
     * physically present in the queue, but it can no longer be read. Therefore, logically, there is no message
     * with `user_id = 123` in the queue. Therefore, a new message can be added without violating uniqueness.
     *
     * However, for the remaining scenarios, there are two reasonable, useful options:
     * 1. All live messages (`SCHEDULED` + `READY` + `IN_FLIGHT` + `RETRY`) must be unique. If a message
     * with `user_id = 123` is just added to the queue (`SCHEDULED`), ready for processing (`READY`), accepted for
     * processing (`IN_FLIGHT`), or awaiting the next processing attempt (`RETRY`), it is still considered alive,
     * since its processing has not yet been completed and we don't want duplicates with the same `user_id` in the queue.
     * This is exactly how the [ALL_LIVE_UNIQUE] option works (i.e., all live messages are unique)
     *
     * 2. Only new messages must be unique (`SCHEDULED` + `READY`). Once a message is accepted for processing for the first
     * time, it is considered "touched" and is no longer subject to uniqueness checks. In this scenario, only messages that have
     * never been processed are unique.
     * This is exactly how the [UNTOUCHED_UNIQUE] option works (i.e., only `SCHEDULED` + `READY` messages are unique)
     *
     * Read more about message states, transitions and deduplication modes in `docs/Message state transitions.md`
     *
     * Implementation details:
     * An unique index is created in the database for each such field. The general rule is that the more indexes, the
     * worse the performance impact, so use this field type only if uniqueness is truly required for the field. Indexes are
     * highly optimized in DBMSs, so there's no need to avoid such fields at all costs; the queue design should be reasonable.
     * If your application truly requires a queue with deduplication for one or more meta fields, add them; don't build
     * strange and fragile workarounds.
     */
    ALL_LIVE_UNIQUE,
    @Deprecated("Use ALL_LIVE_UNIQUE instead", ReplaceWith("ALL_LIVE_UNIQUE"))
    STRICT_UNIQUE,

    /**
     * Field is unique (for "untouched" only messages) and searchable
     *
     * Use this value if the meta field is used for record deduplication. In addition to deduplication, this option provides
     * filtering and sorting, similar to the [SEARCH] option.
     *
     * Any message in a queue is in one of six states:
     * 1. `SCHEDULED` - a message has been placed in the queue, but no attempt has been made to process it; it is still waiting
     * and not yet visible to consumers if [QueueOptions.defaultDelay][kolbasa.queue.QueueOptions.defaultDelay] (and other delays)
     * is set to greater than zero. If delay is zero, the message is immediately moved to the next state `READY`.
     * 2. `READY` - a message is ready to be processed, it is visible to consumers and can be received for processing. The
     * message has not been processed before; this will be the first attempt.
     * 3. `IN_FLIGHT` - a message is being processed right now. It has been received for processing by a
     * consumer, but the processing has not yet been completed.
     * 4. `RETRY` - the processing attempt failed (the visibility timeout expired); the message is available
     * for the next attempt. Like `READY` the message is available for processing; unlike `READY`, there was at least one
     * attempt to process the message.
     * 5. `DEAD` - all attempts to process it were unsuccessful; the message is waiting for sweep to clear it from the queue.
     * 6. `COMPLETED` - the message has been successfully processed and removed from the queue.
     *
     * ```
     *                                           ┌───> DEAD (remaining attempts is 0)
     *                                           │
     *  SCHEDULED ──> READY ──────> IN_FLIGHT ───────> COMPLETED
     *                         ↑                 │
     *                         └───── RETRY <────┘
     * ```
     *
     * For example, we have a `user_id` meta field by which we want to dedupe messages in the queue.
     * There is already a message with `user_id = 123` in the queue, and we want to add another one. How should uniqueness
     * control work depending on the state of the first, already existing message?
     *
     * It's clear that if the first message is `DEAD`, the second one should be added to the queue. A dead message is still
     * physically present in the queue, but it can no longer be read. Therefore, logically, there is no message
     * with `user_id = 123` in the queue. Therefore, a new message can be added without violating uniqueness.
     *
     * However, for the remaining scenarios, there are two reasonable, useful options:
     * 1. All live messages (`SCHEDULED` + `READY` + `IN_FLIGHT` + `RETRY`) must be unique. If a message
     * with `user_id = 123` is just added to the queue (`SCHEDULED`), ready for processing (`READY`), accepted for
     * processing (`IN_FLIGHT`), or awaiting the next processing attempt (`RETRY`), it is still considered alive,
     * since its processing has not yet been completed and we don't want duplicates with the same `user_id` in the queue.
     * This is exactly how the [ALL_LIVE_UNIQUE] option works (i.e., all live messages are unique)
     *
     * 2. Only new messages must be unique (`SCHEDULED` + `READY`). Once a message is accepted for processing for the first
     * time, it is considered "touched" and is no longer subject to uniqueness checks. In this scenario, only messages that have
     * never been processed are unique.
     * This is exactly how the [UNTOUCHED_UNIQUE] option works (i.e., only `SCHEDULED` + `READY` messages are unique)
     *
     * Read more about message states, transitions and deduplication modes in `docs/Message state transitions.md`
     *
     * Implementation details:
     * An unique index is created in the database for each such field. The general rule is that the more indexes, the
     * worse the performance impact, so use this field type only if uniqueness is truly required for the field. Indexes are
     * highly optimized in DBMSs, so there's no need to avoid such fields at all costs; the queue design should be reasonable.
     * If your application truly requires a queue with deduplication for one or more meta fields, add them; don't build
     * strange and fragile workarounds.
     */
    UNTOUCHED_UNIQUE,
    @Deprecated("Use UNTOUCHED_UNIQUE instead", ReplaceWith("UNTOUCHED_UNIQUE"))
    PENDING_ONLY_UNIQUE,

}

