package kolbasa.butcher.config

/**
 * The cluster the config files given with `-c` describe: one endpoint per node, merged from all the files
 */
internal class ClusterConfig(val endpoints: List<PgEndpoint>)
