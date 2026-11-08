package kolbasa.cluster

/**
 * What to do when a scheduled update of the cluster topology fails, for example because a node is down.
 *
 * The [ClusterTopologyUpdater] goes on whatever the handler does: the cluster keeps the old topology, and the next
 * update runs after the usual interval. So this handler only reports the error. The first update, in
 * [ClusterTopologyUpdater.start], doesn't come here: it throws to the caller. A direct call of
 * [Cluster.updateTopology] throws to the caller too.
 *
 * **A handler must not throw.** If it does, the next update is still scheduled, but the exception is lost. If your
 * handler calls code that can fail, for example your own logging or alerting, catch everything inside the handler.
 *
 * The handler is called on [ClusterTopologyUpdaterOptions.executor].
 *
 * By default, [SYSTEM_LOG] is used. Set another one with [ClusterTopologyUpdaterOptions.errorHandler].
 */
fun interface TopologyUpdateErrorHandler {

    /**
     * Called every time a scheduled update fails. Must not throw, see [TopologyUpdateErrorHandler].
     */
    fun onTopologyUpdateError(error: TopologyUpdateError)

    companion object {

        /**
         * Writes a warning to the log. The default.
         *
         * The log is the JDK's [System.Logger] named `kolbasa.cluster.TopologyUpdateErrorHandler`. By default, it
         * writes to `java.util.logging`. To send it to SLF4J or Log4j instead, add `slf4j-jdk-platform-logging` or
         * `log4j-jpl` to the application.
         */
        @JvmField
        val SYSTEM_LOG = TopologyUpdateErrorHandler { error ->
            val message = "Can't update the cluster topology, the old one stays until the next update"
            TopologyUpdateErrorLogSupport.logger.log(System.Logger.Level.WARNING, message, error.cause)
        }
    }
}

private object TopologyUpdateErrorLogSupport {

    val logger: System.Logger = System.getLogger(TopologyUpdateErrorHandler::class.java.name)
}
