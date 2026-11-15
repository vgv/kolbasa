package kolbasa.spring.boot

import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.convert.DurationStyle
import org.springframework.core.env.Environment
import java.time.Duration

/**
 * How many times the starter tries the schema at startup, and how long it waits before every next try.
 *
 * Read from `<prefix>.startup-attempts` and `<prefix>.startup-retry-delay`, see [read].
 */
internal class SchemaStartupAttempts(
    val attempts: Int,
    val retryDelay: Duration
) {

    init {
        require(attempts >= 1) { "At least one attempt is needed, but got $attempts" }
        require(!retryDelay.isNegative) { "The retry delay can't be negative, but got $retryDelay" }
    }

    /**
     * Calls [attempt] until [succeeded] says yes, or the attempts run out, with [retryDelay] before every next call.
     * Returns what the last call returned.
     */
    fun <R> run(attempt: () -> R, succeeded: (R) -> Boolean): R {
        var result = attempt()
        repeat(attempts - 1) {
            if (succeeded(result)) {
                return result
            }
            Thread.sleep(retryDelay.toMillis())
            result = attempt()
        }
        return result
    }

    companion object {

        const val CLUSTER_SCHEMA_PREFIX = "kolbasa.cluster.schema"

        const val DEFAULT_ATTEMPTS = 5
        const val DEFAULT_RETRY_DELAY = "1s"

        /**
         * Reads `<prefix>.startup-attempts` and `<prefix>.startup-retry-delay`. A value that is not allowed stops the
         * application with the name of the property.
         */
        fun read(environment: Environment, prefix: String): SchemaStartupAttempts {
            val binder = Binder.get(environment)
            val attempts = binder.bind("$prefix.startup-attempts", Int::class.javaObjectType).orElse(null)
                ?: DEFAULT_ATTEMPTS
            val retryDelay = binder.bind("$prefix.startup-retry-delay", Duration::class.java).orElse(null)
                ?: DurationStyle.detectAndParse(DEFAULT_RETRY_DELAY)

            require(attempts >= 1) { "$prefix.startup-attempts must be at least 1, but is $attempts" }
            require(!retryDelay.isNegative) { "$prefix.startup-retry-delay can't be negative, but is $retryDelay" }
            return SchemaStartupAttempts(attempts, retryDelay)
        }
    }
}
