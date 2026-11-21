package kolbasa.spring.boot

import kolbasa.consumer.ConsumerOptions
import kolbasa.mutator.MutatorOptions
import kolbasa.producer.ProducerOptions
import kolbasa.queue.Queue

/**
 * Switches kolbasa on in standalone mode: the queues live in one database, the application's own `DataSource` that
 * Spring Boot creates.
 *
 * Declare it as a bean, and the starter creates the tables of [queues] in that database at startup, and the beans that
 * work with them: `Producer`, `Consumer`, `Mutator` and `Inspector`. Without this bean the starter doesn't touch the
 * database at all.
 *
 * With a `MeterRegistry` bean in the application, for example the one Spring Boot Actuator makes, the role beans
 * report their metrics to it. With an `OpenTelemetry` bean and `kolbasa-opentelemetry` on the classpath, the
 * `Producer` and `Consumer` beans trace through it. Options that set their own `metricsConfig` or `tracing` keep them.
 *
 * ## Usage Example
 *
 * ```kotlin
 * @Bean
 * fun kolbasaStandaloneSettings() = KolbasaStandaloneSettings(
 *     queues = listOf(ORDERS, CUSTOMERS),
 *     consumerOptions = ConsumerOptions(consumer = "billing")
 * )
 * ```
 *
 * The same from Java:
 *
 * ```java
 * @Bean
 * KolbasaStandaloneSettings kolbasaStandaloneSettings() {
 *     return KolbasaStandaloneSettings.builder(List.of(ORDERS, CUSTOMERS))
 *         .consumerOptions(ConsumerOptions.builder().consumer("billing").build())
 *         .build();
 * }
 * ```
 */
data class KolbasaStandaloneSettings(
    /**
     * The queues that live in the application's database. Pass only main queues: the dead letter queue and the
     * archive queue of a queue are created together with it.
     *
     * The starter creates or updates their tables at startup, unless `kolbasa.standalone.schema.enabled` is `false`.
     */
    val queues: List<Queue<*>>,

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
    val mutatorOptions: MutatorOptions = MutatorOptions.DEFAULT
) {

    /**
     * Builder for flexible [KolbasaStandaloneSettings] creation, when only some of the options need to be set.
     */
    class Builder internal constructor(private val queues: List<Queue<*>>) {
        private var producerOptions: ProducerOptions = ProducerOptions.DEFAULT
        private var consumerOptions: ConsumerOptions = ConsumerOptions.DEFAULT
        private var mutatorOptions: MutatorOptions = MutatorOptions.DEFAULT

        /**
         * Sets [KolbasaStandaloneSettings.producerOptions] – the options of the `Producer` bean.
         */
        fun producerOptions(producerOptions: ProducerOptions) = apply { this.producerOptions = producerOptions }

        /**
         * Sets [KolbasaStandaloneSettings.consumerOptions] – the options of the `Consumer` bean.
         */
        fun consumerOptions(consumerOptions: ConsumerOptions) = apply { this.consumerOptions = consumerOptions }

        /**
         * Sets [KolbasaStandaloneSettings.mutatorOptions] – the options of the `Mutator` bean.
         */
        fun mutatorOptions(mutatorOptions: MutatorOptions) = apply { this.mutatorOptions = mutatorOptions }

        /**
         * Creates a new [KolbasaStandaloneSettings] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = KolbasaStandaloneSettings(queues, producerOptions, consumerOptions, mutatorOptions)
    }

    companion object {

        /**
         * Starts a [Builder] for these [queues].
         */
        @JvmStatic
        fun builder(queues: List<Queue<*>>) = Builder(queues)
    }
}
