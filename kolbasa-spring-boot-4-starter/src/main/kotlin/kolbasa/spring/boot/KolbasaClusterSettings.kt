package kolbasa.spring.boot

import kolbasa.queue.Queue
import java.util.function.Supplier
import javax.sql.DataSource

/**
 * Switches kolbasa on in a cluster: queues spread over several PostgreSQL nodes, see `kolbasa.cluster.Cluster`.
 *
 * Declare it as a bean, and the starter creates the `Cluster` bean over [nodes]. At startup it reads the cluster
 * topology and creates [queues] on every node, then reads the topology again every minute until the application
 * stops.
 *
 * The cluster doesn't touch the application's own database, the `DataSource` that Spring Boot creates, unless it is
 * one of [nodes]. An application can have both settings beans: [KolbasaStandaloneSettings] for the queues in its own
 * database, and this one for the queues in the cluster.
 *
 * ## Usage Example
 *
 * ```kotlin
 * @Bean
 * fun kolbasaClusterSettings() = KolbasaClusterSettings(
 *     nodes = listOf(nodeA, nodeB, nodeC),
 *     queues = listOf(ORDERS, CUSTOMERS)
 * )
 * ```
 *
 * When nodes or queues can be added while the application runs, pass functions instead of lists:
 * `KolbasaClusterSettings({ currentNodes() }, { listOf(ORDERS, CUSTOMERS) })`.
 */
data class KolbasaClusterSettings(
    /**
     * Returns the `DataSource` of every node. It is called again on every topology update, so a node added to the
     * list is picked up without a restart.
     */
    val nodes: Supplier<List<DataSource>>,

    /**
     * Returns the queues to create or update on every node. Pass only main queues: the dead letter queue and the
     * archive queue of a queue are created together with it. It is called again on every topology update, like
     * [nodes].
     *
     * An empty list means "the schema is mine": the cluster creates no tables, see `kolbasa.cluster.Cluster`.
     */
    val queues: Supplier<List<Queue<*>>>
) {

    /**
     * Settings over a fixed list of nodes and a fixed list of queues.
     */
    constructor(nodes: List<DataSource>, queues: List<Queue<*>>) : this({ nodes }, { queues })
}
