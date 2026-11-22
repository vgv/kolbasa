package kolbasa.spring.boot

import org.springframework.beans.factory.annotation.Qualifier

/**
 * Picks the kolbasa bean of standalone mode, the one that [KolbasaStandaloneSettings] switches on.
 *
 * An application with one settings bean doesn't need it: it has one `Producer` bean, and Spring injects it as is. With
 * both [KolbasaStandaloneSettings] and [KolbasaClusterSettings] it has two beans of every role, so every place that
 * gets one must say which:
 *
 * ```kotlin
 * @Service
 * class OrderService(
 *     @KolbasaStandalone private val outbox: Producer,
 *     @KolbasaCluster private val orders: Producer
 * )
 * ```
 *
 * Without it Spring doesn't know which bean to pick, and the application doesn't start.
 *
 * @see KolbasaCluster
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
@Qualifier
annotation class KolbasaStandalone
