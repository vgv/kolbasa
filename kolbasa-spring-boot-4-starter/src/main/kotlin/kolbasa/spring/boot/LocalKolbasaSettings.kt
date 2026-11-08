package kolbasa.spring.boot

import kolbasa.consumer.ConsumerOptions
import kolbasa.mutator.MutatorOptions
import kolbasa.producer.ProducerOptions
import kolbasa.queue.Queue

/**
 * Switches kolbasa on in the application's own database, the `DataSource` that Spring Boot creates.
 *
 * Declare it as a bean, and the starter creates the tables of [queues] in that database at startup, and the beans that
 * work with them: `Producer`, `Consumer`, `Mutator` and `Inspector`. Without this bean the starter doesn't touch the
 * database at all.
 *
 * ## Usage Example
 *
 * ```kotlin
 * @Bean
 * fun localKolbasaSettings() = LocalKolbasaSettings(
 *     queues = listOf(ORDERS, CUSTOMERS),
 *     consumerOptions = ConsumerOptions(consumer = "billing")
 * )
 * ```
 *
 * The same from Java:
 *
 * ```java
 * @Bean
 * LocalKolbasaSettings localKolbasaSettings() {
 *     return LocalKolbasaSettings.builder(List.of(ORDERS, CUSTOMERS))
 *         .consumerOptions(ConsumerOptions.builder().consumer("billing").build())
 *         .build();
 * }
 * ```
 */
data class LocalKolbasaSettings(
    /**
     * The queues that live in the application's database. Pass only main queues: the dead letter queue and the
     * archive queue of a queue are created together with it.
     *
     * The starter creates or updates their tables at startup, unless `kolbasa.schema.enabled` is `false`.
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
     * Builder for flexible [LocalKolbasaSettings] creation, when only some of the options need to be set.
     */
    class Builder internal constructor(private val queues: List<Queue<*>>) {
        private var producerOptions: ProducerOptions = ProducerOptions.DEFAULT
        private var consumerOptions: ConsumerOptions = ConsumerOptions.DEFAULT
        private var mutatorOptions: MutatorOptions = MutatorOptions.DEFAULT

        /**
         * Sets [LocalKolbasaSettings.producerOptions] – the options of the `Producer` bean.
         */
        fun producerOptions(producerOptions: ProducerOptions) = apply { this.producerOptions = producerOptions }

        /**
         * Sets [LocalKolbasaSettings.consumerOptions] – the options of the `Consumer` bean.
         */
        fun consumerOptions(consumerOptions: ConsumerOptions) = apply { this.consumerOptions = consumerOptions }

        /**
         * Sets [LocalKolbasaSettings.mutatorOptions] – the options of the `Mutator` bean.
         */
        fun mutatorOptions(mutatorOptions: MutatorOptions) = apply { this.mutatorOptions = mutatorOptions }

        /**
         * Creates a new [LocalKolbasaSettings] instance: a fresh one on every call, nothing is cached or reused.
         */
        fun build() = LocalKolbasaSettings(queues, producerOptions, consumerOptions, mutatorOptions)
    }

    companion object {

        /**
         * Starts a [Builder] for these [queues].
         */
        @JvmStatic
        fun builder(queues: List<Queue<*>>) = Builder(queues)
    }
}
