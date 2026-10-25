package kolbasa.consumer

import kolbasa.producer.Id
import kolbasa.producer.ShardId
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.NodeId
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

class ReadErrorTest {

    @Test
    fun testIsLastAttempt() {
        val lastError = ReadError(
            queue = Queue.of("test", PredefinedDataTypes.String),
            messageId = Id(1, ShardId.of(1)),
            nodeId = NodeId.EMPTY_NODE_ID,
            createdAt = Instant.EPOCH,
            processingAt = Instant.EPOCH,
            scheduledAt = Instant.EPOCH,
            remainingAttempts = 0,
            cause = IllegalArgumentException("Can't read")
        )
        assertTrue(lastError.isLastAttempt(), "Error: $lastError")

        val notLastError = lastError.copy(remainingAttempts = 1)
        assertFalse(notLastError.isLastAttempt(), "Error: $notLastError")
    }

}
