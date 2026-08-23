package kolbasa.producer

import kolbasa.queue.meta.MetaValues

/**
 * One message to send, together with its metadata and its per-message options.
 *
 * This is the unit every producer works with: [Producer.send][kolbasa.producer.datasource.Producer.send] accepts a single
 * message, a list of messages, or a [SendRequest] that wraps a list together with options for the whole send() call.
 *
 * A message carries three things, and only the first one is required:
 * - (1) [data] – the payload, stored in the queue's data column
 * - (2) [meta] – metadata values, used to filter and sort messages when they are received
 * - (3) [options] – delay and attempts overrides for this particular message
 *
 * `Data` must match the type of the queue the message is sent to – a `Queue<String>` accepts `SendMessage<String>` only.
 *
 * Usage example (Kotlin):
 *
 * ```kotlin
 * val USER_ID = MetaField.ofInt("user_id", FieldOption.SEARCH)
 *
 * producer.send(queue, listOf(
 *     SendMessage("just a payload"),
 *     SendMessage("with metadata", MetaValues.of(USER_ID.value(42))),
 *     SendMessage("delayed by five minutes", MessageOptions(delay = Duration.ofMinutes(5))),
 *     SendMessage("both", MetaValues.of(USER_ID.value(42)), MessageOptions(attempts = 10))
 * ))
 * ```
 *
 * Usage example (Java):
 *
 * ```java
 * producer.send(queue, List.of(
 *     new SendMessage<>("just a payload"),
 *     new SendMessage<>("with metadata", MetaValues.of(USER_ID.value(42))),
 *     new SendMessage<>("delayed by five minutes", MessageOptions.builder().delay(Duration.ofMinutes(5)).build()),
 *     new SendMessage<>("both", MetaValues.of(USER_ID.value(42)), MessageOptions.builder().attempts(10).build())
 * ));
 * ```
 *
 * See also [SendRequest] to send a batch of messages with options for the whole send() call, [MetaValues] for
 * building the metadata values of a message, and [MessageOptions] for per-message delay and attempts.
 *
 * @constructor Creates a message from all three parts. The secondary constructors below cover the cases where the
 * metadata, the options, or both are left at their defaults.
 */
data class SendMessage<Data>(
    /**
     * The message payload.
     *
     * It is written into the queue's data column, serialized by the queue's
     * [DatabaseQueueDataType][kolbasa.queue.DatabaseQueueDataType] – which is why the type of the message and the type of
     * the queue have to agree.
     *
     * The payload is what your business code receives back from
     * [Consumer.receive][kolbasa.consumer.datasource.Consumer.receive] as [Message.data][kolbasa.consumer.Message.data].
     * Data you want to filter or sort by belongs in [meta] instead – see
     * [Queue.metadata][kolbasa.queue.Queue.metadata] for how to decide which of the two a particular field goes into.
     */
    val data: Data,

    /**
     * Metadata values for this message.
     *
     * Metadata is stored in its own database columns, next to the message, and it is the only part of a message that
     * consumers can filter and sort by. Use [MetaValues.EMPTY][kolbasa.queue.meta.MetaValues.EMPTY] when a message has no
     * metadata at all.
     *
     * Every value here belongs to a [MetaField][kolbasa.queue.meta.MetaField] declared in the queue's
     * [Metadata][kolbasa.queue.meta.Metadata], so the fields you use have to be the ones the target queue was built with.
     *
     * Fields the message does not mention are stored as NULL, which is the normal case for optional metadata.
     */
    val meta: MetaValues,

    /**
     * Delay and attempts for this particular message.
     *
     * This is the most granular level of the options hierarchy and it overrides every other level:
     *
     * ```
     * QueueOptions (lowest) → ProducerOptions → SendOptions → MessageOptions (highest)
     * ```
     *
     * Use [MessageOptions.DEFAULT][kolbasa.producer.MessageOptions.DEFAULT] – as the constructors that omit this parameter
     * do – to leave both knobs unset, so the message inherits whatever the send() call, the producer and the queue define.
     * Setting options here is what makes a single message arrive later than the rest of its batch, or get more delivery
     * attempts than the queue would normally allow.
     */
    val options: MessageOptions
) {

    /**
     * Creates a message with no metadata and no per-message options.
     *
     * The most common form: delay and attempts are inherited from the send() call, the producer and the queue, and the
     * message carries no metadata to filter or sort by.
     */
    constructor(data: Data) : this(data, MetaValues.EMPTY, MessageOptions.DEFAULT)

    /**
     * Creates a message with metadata, but with no per-message options.
     *
     * Delay and attempts are inherited from the send() call, the producer and the queue.
     */
    constructor(data: Data, meta: MetaValues) : this(data, meta, MessageOptions.DEFAULT)

    /**
     * Creates a message with per-message options, but with no metadata.
     *
     * The message is stored with all its metadata columns NULL, and [options] overrides the delay and attempts coming
     * from the send() call, the producer and the queue.
     */
    constructor(data: Data, options: MessageOptions) : this(data, MetaValues.EMPTY, options)
}
