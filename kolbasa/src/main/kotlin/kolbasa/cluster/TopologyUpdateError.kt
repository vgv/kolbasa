package kolbasa.cluster

import kolbasa.schema.NodeId
import kolbasa.schema.SchemaResult

/**
 * A topology update that went wrong, see [TopologyUpdateErrorHandler].
 *
 * There are two kinds. [UpdateFailed]: the update threw, and the old topology stays. [SchemaFailed]: the new topology
 * is in use, but the queue tables couldn't be created or updated on some nodes.
 */
sealed class TopologyUpdateError {

    /**
     * The update threw, for example because a node is down. The old topology stays until the next update.
     */
    data class UpdateFailed(
        /**
         * What the update threw.
         */
        val cause: Exception
    ) : TopologyUpdateError()

    /**
     * The new topology is in use, but some statements of the schema failed, see [Cluster.updateTopology]. The next
     * update runs the schema again, until all statements go through.
     */
    data class SchemaFailed(
        /**
         * The nodes where the schema failed, each with its [SchemaResult].
         */
        val failures: Map<NodeId, SchemaResult>
    ) : TopologyUpdateError()
}
