package kolbasa.consumer

data class Message<V, Meta>(
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
    val data: V,
    /**
     * Metadata, if any
     */
    val meta: Meta?
)
