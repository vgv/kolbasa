package kolbasa.butcher

import com.github.ajalt.clikt.parameters.options.check
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import kolbasa.cluster.admin.ClusterAdmin
import kolbasa.cluster.admin.ClusterOperationException
import kolbasa.producer.ShardId

/**
 * A command that carries a whole plan out, as a loop of the low-level steps
 *
 * What the two of them share: how big a round is, and the refusal to start on top of a migration.
 */
internal abstract class HighLevelCommand(name: String) : MutatingClusterCommand(name) {

    final override val helpSection = HelpSection.HIGH_LEVEL

    protected val shardsPerRound: Int by option("--shards-per-round", metavar = "<n>")
        .int()
        .default(1)
        .help("How many shards to move in one round. 1 by default")
        .check({ "shards per round must be between 1 and ${ShardId.SHARD_COUNT}, but was $it" }) { shards ->
            shards in 1..ShardId.SHARD_COUNT
        }

    /**
     * Refuses to go on while any shard is migrating
     *
     * The loop in the library makes the same refusal, but only once it has found something to move. Asked
     * for a plan it cannot improve on, a command would otherwise report "nothing to do" and say nothing
     * about shards that no node is consuming.
     */
    protected fun refuseIfMigrating(butcher: ClusterAdmin) {
        val migrationState = butcher.checkMigrationState()

        if (!migrationState.isClean) {
            throw ClusterOperationException.MigrationAlreadyInFlightException(migrationState.migratingShardsByTarget)
        }
    }
}
