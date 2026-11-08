package kolbasa.cluster

/**
 * A scheduled update of the cluster topology that failed, see [TopologyUpdateErrorHandler].
 */
data class TopologyUpdateError(
    /**
     * What the update threw.
     */
    val cause: Exception
)
