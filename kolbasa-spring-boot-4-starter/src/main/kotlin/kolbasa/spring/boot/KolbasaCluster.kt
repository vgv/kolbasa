package kolbasa.spring.boot

import org.springframework.beans.factory.annotation.Qualifier

/**
 * Picks the kolbasa bean of the cluster, the one that [KolbasaClusterSettings] switches on.
 *
 * Needed only in an application with both [KolbasaStandaloneSettings] and [KolbasaClusterSettings], see
 * [KolbasaStandalone].
 *
 * @see KolbasaStandalone
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
@Qualifier
annotation class KolbasaCluster
