package kolbasa.cluster

/**
 * A topology update that went wrong, see [TopologyUpdateErrorHandler].
 *
 * There are two kinds. [UpdateFailed]: the update threw, and the topology in use stays. [SchemaFailed]: the new
 * topology is in use, but the queue tables couldn't be created or updated on some nodes.
 */
sealed class TopologyUpdateError {

    /**
     * The update threw, for example because a node is down. The topology in use stays until the next update: usually
     * the old one. If a node couldn't take the rows of the shard map, which is the last step of an update, it is the
     * new one.
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
         * What the schema did on every node. [ClusterSchemaResult.failedNodes] are the nodes where it failed.
         */
        val result: ClusterSchemaResult
    ) : TopologyUpdateError()
}
