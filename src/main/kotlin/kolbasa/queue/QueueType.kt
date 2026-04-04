package kolbasa.queue

/**
 * Defines the role of a [Queue].
 *
 * Every queue has exactly one type that determines how messages enter and leave it.
 * Users can only create [MAIN] queues directly. [DLQ] and [ARCHIVE] queues are
 * created automatically by the library when the corresponding options are enabled
 * in [QueueOptions].
 */
enum class QueueType {
    /**
     * A standard queue used for normal message processing.
     *
     * Messages are sent by producers, received by consumers, and deleted after
     * successful processing. This is the only type that users can create directly.
     */
    MAIN,

    /**
     * A dead-letter queue that collects messages which have exhausted all processing attempts.
     *
     * Instead of being permanently deleted by sweep, dead messages are atomically moved
     * to this queue for inspection, debugging, or reprocessing. Created automatically
     * when [QueueOptions.dlqOptions] is set on a [MAIN] queue. The corresponding
     * table uses the `_dlq` suffix (e.g. `q_orders_dlq`).
     */
    DLQ,

    /**
     * An archive queue that stores successfully processed messages.
     *
     * Instead of being permanently deleted on [Consumer.delete()][kolbasa.consumer.datasource.Consumer.delete],
     * messages are atomically moved to this queue for auditing, trailing, or compliance purposes.
     * Created automatically when [QueueOptions.archiveQueueOptions] is set on a [MAIN] queue.
     * The corresponding table uses the `_arc` suffix (e.g. `q_orders_arc`).
     */
    ARCHIVE
}
