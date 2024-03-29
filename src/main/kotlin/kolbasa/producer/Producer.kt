package kolbasa.producer

/**
 * Base interface for all producers
 *
 * Producers are used to send messages to the queues.
 *
 * This is a basic interface, implementations of which may behave differently. Kolbasa provides a default, high-performance
 * implementation of [Producer], which uses just plain JDBC and [DataSource][javax.sql.DataSource] and doesn't require
 * any additional dependencies.
 * This default, provided implementation completely hides database handling and transaction management from the user.
 *
 * If you require a producer working in context of already opened transaction, please, look at [ConnectionAwareProducer].
 *
 * @param Data type of the message
 * @param Meta type of the metadata
 */
interface Producer<Data, Meta : Any> {

    /**
     * Just to send one message without metadata and another options
     *
     * @param data message to send
     * @return if success - unique id of the message; if error - throws an exception; if duplicate -
     * [Const.RESERVED_DUPLICATE_ID][kolbasa.schema.Const.RESERVED_DUPLICATE_ID]
     */
    fun send(data: Data): Long

    /**
     * Send one message with optional metadata and [MessageOptions]
     *
     * @param data message, metadata (if any) and options (if any) to send
     * @return if success - unique id of the message; if error - throws an exception; if duplicate -
     * [Const.RESERVED_DUPLICATE_ID][kolbasa.schema.Const.RESERVED_DUPLICATE_ID]
     */
    fun send(data: SendMessage<Data, Meta>): Long

    /**
     * Send many messages with optional metadata and [MessageOptions] defined for every message
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param data list of messages, metadata (if any) and options (if any) to send
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun send(data: List<SendMessage<Data, Meta>>): SendResult<Data, Meta>

    /**
     * Send many messages with optional metadata and [MessageOptions] defined for every message and custom [SendOptions]
     *
     * This is the most effective way to send a lot of messages due to the batching and another optimizations.
     *
     * @param data list of messages, metadata (if any) and options (if any) to send
     * @param sendOptions options for sending this list of messages, allows to override global [Producer] options
     * @return [SendResult] with the list of failed messages and the list of successful messages
     */
    fun send(data: List<SendMessage<Data, Meta>>, sendOptions: SendOptions): SendResult<Data, Meta>
}
