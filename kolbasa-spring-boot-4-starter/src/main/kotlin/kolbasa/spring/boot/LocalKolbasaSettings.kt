package kolbasa.spring.boot

import kolbasa.queue.Queue

/**
 * Switches kolbasa on in the application's own database, the `DataSource` that Spring Boot creates.
 *
 * Declare it as a bean, and the starter creates the tables of [queues] in that database at startup. Without this bean
 * the starter doesn't touch the database at all.
 *
 * ## Usage Example
 *
 * ```kotlin
 * @Bean
 * fun localKolbasaSettings() = LocalKolbasaSettings(queues = listOf(ORDERS, CUSTOMERS))
 * ```
 *
 * The same from Java:
 *
 * ```java
 * @Bean
 * LocalKolbasaSettings localKolbasaSettings() {
 *     return new LocalKolbasaSettings(List.of(ORDERS, CUSTOMERS));
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
    val queues: List<Queue<*>>
)
