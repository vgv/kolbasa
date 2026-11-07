package kolbasa.cluster

/**
 * What to do when a scheduled update of the cluster state fails, for example because a node is down.
 *
 * The [Cluster] goes on whatever the handler does: it keeps the old state and tries again after the usual interval.
 * So this handler only reports the error. The first update, in [Cluster.initAndScheduleStateUpdate], doesn't come
 * here: it throws to the caller. A direct call of [Cluster.updateStateOnce] throws to the caller too.
 *
 * **A handler must not throw.** If it does, the next update is still scheduled, but the exception is lost. If your
 * handler calls code that can fail, for example your own logging or alerting, catch everything inside the handler.
 *
 * The handler is called on the thread that runs the scheduled updates.
 *
 * By default, [SYSTEM_LOG] is used.
 */
fun interface StateUpdateErrorHandler {

    /**
     * Called every time a scheduled update fails. Must not throw, see [StateUpdateErrorHandler].
     */
    fun onStateUpdateError(error: StateUpdateError)

    companion object {

        /**
         * Writes a warning to the log. The default.
         *
         * The log is the JDK's [System.Logger] named `kolbasa.cluster.StateUpdateErrorHandler`. By default, it
         * writes to `java.util.logging`. To send it to SLF4J or Log4j instead, add `slf4j-jdk-platform-logging` or
         * `log4j-jpl` to the application.
         */
        @JvmField
        val SYSTEM_LOG = StateUpdateErrorHandler { error ->
            val message = "Can't update the cluster state, the old one stays until the next update"
            StateUpdateErrorLogSupport.logger.log(System.Logger.Level.WARNING, message, error.cause)
        }
    }
}

private object StateUpdateErrorLogSupport {

    val logger: System.Logger = System.getLogger(StateUpdateErrorHandler::class.java.name)
}
