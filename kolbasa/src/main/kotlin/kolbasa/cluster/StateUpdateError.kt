package kolbasa.cluster

/**
 * A scheduled update of the cluster state that failed, see [StateUpdateErrorHandler].
 */
data class StateUpdateError(
    /**
     * What the update threw.
     */
    val cause: Exception
)
