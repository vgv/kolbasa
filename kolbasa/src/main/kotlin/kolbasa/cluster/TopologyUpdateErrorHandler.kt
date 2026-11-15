package kolbasa.cluster

import kolbasa.schema.NodeId
import kolbasa.schema.SchemaResult

/**
 * What to do when an update of the cluster topology goes wrong: a node is down, or the queue tables can't be created
 * on some nodes. The two cases are the two kinds of [TopologyUpdateError].
 *
 * The [ClusterTopologyUpdater] goes on whatever the handler does, and the next update runs after the usual interval.
 * So this handler only reports the error.
 *
 * - [TopologyUpdateError.UpdateFailed] comes from the scheduled updates only. The first update, in
 *   [ClusterTopologyUpdater.start], throws to the caller instead: without a topology the cluster can't work.
 * - [TopologyUpdateError.SchemaFailed] comes from the first update too: the topology is in use, so `start()` doesn't
 *   throw for it.
 *
 * A direct call of [Cluster.updateTopology] doesn't come here at all: it throws, or returns the schema failures in its
 * [ClusterSchemaResult].
 *
 * **A handler must not throw.** If it does, the next update is still scheduled, but the exception is lost. If your
 * handler calls code that can fail, for example your own logging or alerting, catch everything inside the handler.
 *
 * The handler is called on [ClusterTopologyUpdaterOptions.executor], and for the first update on the thread that calls
 * [ClusterTopologyUpdater.start].
 *
 * By default, [SYSTEM_LOG] is used. Set another one with [ClusterTopologyUpdaterOptions.errorHandler].
 */
fun interface TopologyUpdateErrorHandler {

    /**
     * Called every time an update goes wrong. Must not throw, see [TopologyUpdateErrorHandler].
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
            when (error) {
                is TopologyUpdateError.UpdateFailed -> {
                    val message = "Can't update the cluster topology, the old one stays until the next update"
                    TopologyUpdateErrorLogSupport.logger.log(System.Logger.Level.WARNING, message, error.cause)
                }

                is TopologyUpdateError.SchemaFailed -> {
                    val message = "Can't create the tables of kolbasa queues on some nodes, the next topology update " +
                        "tries again:\n" + TopologyUpdateErrorLogSupport.describe(error.result.failedNodes)
                    TopologyUpdateErrorLogSupport.logger.log(System.Logger.Level.WARNING, message)
                }
            }
        }
    }
}

private object TopologyUpdateErrorLogSupport {

    val logger: System.Logger = System.getLogger(TopologyUpdateErrorHandler::class.java.name)

    // Every failed statement under its node, the same way the Spring Boot starter lists them
    fun describe(failures: Map<NodeId, SchemaResult>): String {
        return failures.entries.joinToString("\n") { (node, result) ->
            val failed = result.failedTableStatements + result.failedIndexStatements
            "Node ${node.id}:\n" + failed.joinToString("\n") { "${it.statement}\n  -> ${it.error.message}" }
        }
    }
}
