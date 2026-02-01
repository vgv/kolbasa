package kolbasa.queue.meta

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
     * is set to greater than zero. If delay is zero, the message is immediately moved to the next state `AVAILABLE`.
     * 2. `AVAILABLE` - a message is ready to be processed, it is visible to consumers and can be received for processing.
     * 3. `IN_FLIGHT` - a message is being processed right now. It has been received for processing by a
     * consumer, but the processing has not yet been completed.
     * 4. `RETRY_SCHEDULED` - similar to `SCHEDULED`, but it has already been processed at least once; after an unsuccessful
     * processing attempt, the message is waiting for the next attempt.
     * 5. `DEAD` - all attempts to process it were unsuccessful; the message is waiting for sweep to clear it from the queue.
     * 6. `COMPLETED` - the message has been successfully processed and removed from the queue.
     *
     * ```
     *                                                ┌───> DEAD (remaining attempts is 0)
     *                                                │
     *  SCHEDULED ─────> AVAILABLE ─────> IN_FLIGHT ─────> COMPLETED
     *              ↑                                 │
     *              └─────── RETRY_SCHEDULED <────────┘
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
     * 1. All live messages (`SCHEDULED` + `AVAILABLE` + `IN_FLIGHT` + `RETRY_SCHEDULED`) must be unique. If a message
     * with `user_id = 123` is just added to the queue (`SCHEDULED`), available for processing (`AVAILABLE`), accepted for
     * processing (`IN_FLIGHT`), or awaiting the next processing attempt (`RETRY_SCHEDULED`), it is still considered alive,
     * since its processing has not yet been completed and we don't want duplicates with the same `user_id` in the queue.
     * This is exactly how the [ALL_LIVE_UNIQUE] option works (i.e., all live messages are unique)
     *
     * 2. Only new messages must be unique (`SCHEDULED` + `AVAILABLE`). Once a message is accepted for processing for the first
     * time, it is considered "touched" and is no longer subject to uniqueness checks. In this scenario, only messages that have
     * never been processed are unique.
     * This is exactly how the [UNTOUCHED_UNIQUE] option works (i.e., only `SCHEDULED` + `AVAILABLE` messages are unique)
     *
     * Implementation details:
     * An unique index is created in the database for each such field. The general rule is that the more indexes, the
     * worse the performance impact, so use this field type only if uniqueness is truly required for the field. Indexes are
     * highly optimized in DBMSs, so there's no need to avoid such fields at all costs; the queue design should be reasonable.
     * If your application truly requires a queue with deduplication for one or more meta fields, add them; don't build
     * strange and fragile workarounds.
     */
    ALL_LIVE_UNIQUE,
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
     * is set to greater than zero. If delay is zero, the message is immediately moved to the next state `AVAILABLE`.
     * 2. `AVAILABLE` - a message is ready to be processed, it is visible to consumers and can be received for processing.
     * 3. `IN_FLIGHT` - a message is being processed right now. It has been received for processing by a
     * consumer, but the processing has not yet been completed.
     * 4. `RETRY_SCHEDULED` - similar to `SCHEDULED`, but it has already been processed at least once; after an unsuccessful
     * processing attempt, the message is waiting for the next attempt.
     * 5. `DEAD` - all attempts to process it were unsuccessful; the message is waiting for sweep to clear it from the queue.
     * 6. `COMPLETED` - the message has been successfully processed and removed from the queue.
     *
     * ```
     *                                                ┌───> DEAD (remaining attempts is 0)
     *                                                │
     *  SCHEDULED ─────> AVAILABLE ─────> IN_FLIGHT ─────> COMPLETED
     *              ↑                                 │
     *              └─────── RETRY_SCHEDULED <────────┘
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
     * 1. All live messages (`SCHEDULED` + `AVAILABLE` + `IN_FLIGHT` + `RETRY_SCHEDULED`) must be unique. If a message
     * with `user_id = 123` is just added to the queue (`SCHEDULED`), available for processing (`AVAILABLE`), accepted for
     * processing (`IN_FLIGHT`), or awaiting the next processing attempt (`RETRY_SCHEDULED`), it is still considered alive,
     * since its processing has not yet been completed and we don't want duplicates with the same `user_id` in the queue.
     * This is exactly how the [ALL_LIVE_UNIQUE] option works (i.e., all live messages are unique)
     *
     * 2. Only new messages must be unique (`SCHEDULED` + `AVAILABLE`). Once a message is accepted for processing for the first
     * time, it is considered "touched" and is no longer subject to uniqueness checks. In this scenario, only messages that have
     * never been processed are unique.
     * This is exactly how the [UNTOUCHED_UNIQUE] option works (i.e., only `SCHEDULED` + `AVAILABLE` messages are unique)
     *
     * Implementation details:
     * An unique index is created in the database for each such field. The general rule is that the more indexes, the
     * worse the performance impact, so use this field type only if uniqueness is truly required for the field. Indexes are
     * highly optimized in DBMSs, so there's no need to avoid such fields at all costs; the queue design should be reasonable.
     * If your application truly requires a queue with deduplication for one or more meta fields, add them; don't build
     * strange and fragile workarounds.
     */
    PENDING_ONLY_UNIQUE,
    UNTOUCHED_UNIQUE,
}

