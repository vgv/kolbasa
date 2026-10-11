package kolbasa.butcher.cli.config

/**
 * The command line or a cluster config file does not make sense
 */
internal class InvalidConfigurationException(override val message: String) : RuntimeException(message)
