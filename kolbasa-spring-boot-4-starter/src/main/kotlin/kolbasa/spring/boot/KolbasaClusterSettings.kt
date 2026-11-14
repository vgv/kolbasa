package kolbasa.spring.boot

import kolbasa.cluster.ClusterTopologyUpdaterOptions
import kolbasa.cluster.ShardStrategy
import kolbasa.consumer.ConsumerOptions
import kolbasa.mutator.MutatorOptions
import kolbasa.producer.ProducerOptions
import kolbasa.queue.Queue
import java.util.function.Supplier
import javax.sql.DataSource

/**
 * Switches kolbasa on in a cluster: queues spread over several PostgreSQL nodes, see `kolbasa.cluster.Cluster`.
 *
 * Declare it as a bean, and the starter creates the `Cluster` bean over [nodes], and the beans that work with it:
 * `Producer`, `Consumer`, `Mutator` and `Inspector`. At startup it reads the cluster topology and creates [queues] on
 * every node, then reads the topology again on a schedule, every minute by default, until the application stops.
 *
 * The cluster doesn't touch the application's own database, the `DataSource` that Spring Boot creates, unless it is
 * one of [nodes]. An application can have both settings beans: [KolbasaStandaloneSettings] for the queues in its own
 * database, and this one for the queues in the cluster. Then it has two beans of every role, see [KolbasaStandalone].
 *
 * With a `MeterRegistry` bean in the application, for example the one Spring Boot Actuator makes, the role beans
 * report their metrics to it. Options that set their own `metricsConfig` keep it.
 *
 * ## Usage Example
 *
 * ```kotlin
 * @Bean
 * fun kolbasaClusterSettings() = KolbasaClusterSettings(
 *     nodes = listOf(nodeA, nodeB, nodeC),
 *     queues = listOf(ORDERS, CUSTOMERS),
 *     consumerOptions = ConsumerOptions(consumer = "billing")
 * )
 * ```
 *
 * When nodes or queues can be added while the application runs, pass functions instead of lists:
 * `KolbasaClusterSettings({ currentNodes() }, { listOf(ORDERS, CUSTOMERS) })`.
 *
 * The same from Java:
 *
 * ```java
 * @Bean
 * KolbasaClusterSettings kolbasaClusterSettings() {
 *     return KolbasaClusterSettings.builder(List.of(nodeA, nodeB, nodeC), List.of(ORDERS, CUSTOMERS))
 *         .consumerOptions(ConsumerOptions.builder().consumer("billing").build())
 *         .build();
 * }
 * ```
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
    val queues: Supplier<List<Queue<*>>>,

    /**
     * Options of the `Producer` bean. By default, [ProducerOptions.DEFAULT].
     */
    val producerOptions: ProducerOptions = ProducerOptions.DEFAULT,

    /**
     * Options of the `Consumer` bean. By default, [ConsumerOptions.DEFAULT].
     */
    val consumerOptions: ConsumerOptions = ConsumerOptions.DEFAULT,

    /**
     * Options of the `Mutator` bean. By default, [MutatorOptions.DEFAULT].
     */
    val mutatorOptions: MutatorOptions = MutatorOptions.DEFAULT,

    /**
     * How the `Producer` bean picks the shard of a message, and with it the node. By default,
     * [ShardStrategy.ThreadLocalWithInterval], the same as `ClusterProducer`'s.
     */
    val shardStrategy: ShardStrategy = ShardStrategy.ThreadLocalWithInterval(),

    /**
     * How often the topology is read again, on what executor, and what to do when a read fails. By default,
     * [ClusterTopologyUpdaterOptions.DEFAULT]: every minute.
     */
    val topologyUpdaterOptions: ClusterTopologyUpdaterOptions = ClusterTopologyUpdaterOptions.DEFAULT
) {

    /**
     * Settings over a fixed list of nodes and a fixed list of queues.
     */
    constructor(
        nodes: List<DataSource>,
        queues: List<Queue<*>>,
        producerOptions: ProducerOptions = ProducerOptions.DEFAULT,
        consumerOptions: ConsumerOptions = ConsumerOptions.DEFAULT,
        mutatorOptions: MutatorOptions = MutatorOptions.DEFAULT,
        shardStrategy: ShardStrategy = ShardStrategy.ThreadLocalWithInterval(),
        topologyUpdaterOptions: ClusterTopologyUpdaterOptions = ClusterTopologyUpdaterOptions.DEFAULT
    ) : this(
        { nodes },
        { queues },
        producerOptions,
        consumerOptions,
        mutatorOptions,
        shardStrategy,
        topologyUpdaterOptions
    )

    /**
     * Builder for flexible [KolbasaClusterSettings] creation, when only some of the options need to be set.
     */
    class Builder internal constructor(
        private val nodes: Supplier<List<DataSource>>,
        private val queues: Supplier<List<Queue<*>>>
    ) {
        private var producerOptions: ProducerOptions = ProducerOptions.DEFAULT
        private var consumerOptions: ConsumerOptions = ConsumerOptions.DEFAULT
        private var mutatorOptions: MutatorOptions = MutatorOptions.DEFAULT
        private var shardStrategy: ShardStrategy = ShardStrategy.ThreadLocalWithInterval()
        private var topologyUpdaterOptions: ClusterTopologyUpdaterOptions = ClusterTopologyUpdaterOptions.DEFAULT

        /**
         * Sets [KolbasaClusterSettings.producerOptions] – the options of the `Producer` bean.
         */
        fun producerOptions(producerOptions: ProducerOptions) = apply { this.producerOptions = producerOptions }

        /**
         * Sets [KolbasaClusterSettings.consumerOptions] – the options of the `Consumer` bean.
         */
        fun consumerOptions(consumerOptions: ConsumerOptions) = apply { this.consumerOptions = consumerOptions }

        /**
         * Sets [KolbasaClusterSettings.mutatorOptions] – the options of the `Mutator` bean.
         */
        fun mutatorOptions(mutatorOptions: MutatorOptions) = apply { this.mutatorOptions = mutatorOptions }

        /**
         * Sets [KolbasaClusterSettings.shardStrategy] – how the `Producer` bean picks the shard of a message.
         */
        fun shardStrategy(shardStrategy: ShardStrategy) = apply { this.shardStrategy = shardStrategy }

        /**
         * Sets [KolbasaClusterSettings.topologyUpdaterOptions] – how often the topology is read again.
         */
        fun topologyUpdaterOptions(topologyUpdaterOptions: ClusterTopologyUpdaterOptions) = apply {
            this.topologyUpdaterOptions = topologyUpdaterOptions
        }

        /**
         * Creates a new [KolbasaClusterSettings] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = KolbasaClusterSettings(
            nodes,
            queues,
            producerOptions,
            consumerOptions,
            mutatorOptions,
            shardStrategy,
            topologyUpdaterOptions
        )
    }

    companion object {

        /**
         * Starts a [Builder] over a fixed list of nodes and a fixed list of queues.
         */
        @JvmStatic
        fun builder(nodes: List<DataSource>, queues: List<Queue<*>>) = Builder({ nodes }, { queues })

        /**
         * Starts a [Builder] over functions that return the nodes and the queues, see [KolbasaClusterSettings.nodes].
         */
        @JvmStatic
        fun builder(nodes: Supplier<List<DataSource>>, queues: Supplier<List<Queue<*>>>) = Builder(nodes, queues)
    }
}
