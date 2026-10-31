package kolbasa

import org.junit.jupiter.api.Assertions.assertEquals
import java.sql.Connection

// One method of a role interface: its name and parameter types
private data class Signature(val name: String, val parameterTypes: List<Class<*>>) {
    override fun toString() = "$name(${parameterTypes.joinToString { it.simpleName }})"
}

// Checks that a role that manages its own connections, like Producer, and its connection-aware twin, like
// ConnectionAwareProducer, have the same methods: the same names and the same parameter types, except the leading
// Connection. An async method counts as its sync twin, so sendAsync(queue, request) is send(queue, request)
internal fun assertSameMethods(dataSourceRole: Class<*>, connectionAwareRole: Class<*>) {
    val dataSource = dataSourceRole.declaredMethods
        .map { method -> Signature(method.name.removeSuffix("Async"), method.parameterTypes.toList()) }
        .toSet()

    val connectionAware = connectionAwareRole.declaredMethods
        .map { method ->
            val parameterTypes = method.parameterTypes.toList()
            if (parameterTypes.firstOrNull() == Connection::class.java) {
                Signature(method.name, parameterTypes.drop(1))
            } else {
                Signature(method.name, parameterTypes)
            }
        }
        .toSet()

    assertEquals(emptySet<Signature>(), connectionAware - dataSource, "Only in ${connectionAwareRole.simpleName}")
    assertEquals(emptySet<Signature>(), dataSource - connectionAware, "Only in ${dataSourceRole.simpleName}")
}
