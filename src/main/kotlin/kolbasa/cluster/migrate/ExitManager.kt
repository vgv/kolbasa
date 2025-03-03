package kolbasa.cluster.migrate

import kotlin.system.exitProcess

internal interface ExitManager {
    fun exitWithError()
}

internal object RealExitManager : ExitManager {
    override fun exitWithError() {
        exitProcess(1)
    }
}
