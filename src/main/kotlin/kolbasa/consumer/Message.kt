package kolbasa.consumer

data class Message<Data, Meta>(
    /**
     * Unique message identifier
     */
    val id: Long,
    /**
     * Timestamp when this message was created
     */
    val createdAt: Long,
    /**
     * Timestamp when this message was taken for processing
     */
    val processingAt: Long,
    /**
     * How many attempts we have to process this message
     */
    val remainingAttempts: Int,
    /**
     * Message data
     */
    val data: Data,
    /**
     * Metadata, if
     * 1) There is metadata attached to the message
     * 2) [ReceiveOptions.readMetadata][kolbasa.consumer.ReceiveOptions.readMetadata] set to true
     */
    val meta: Meta?
)
