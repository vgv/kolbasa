package kolbasa

import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract

/**
 * Default JUnit 6 method uses contracts, but doesn't return the actual value.
 * This wrapper fixes that (inspired by the kotlin-test)
 */
@OptIn(ExperimentalContracts::class)
fun <T : Any> assertNotNull(actual: T?, message: String? = null): T {
    contract {
        returns() implies (actual != null)
    }

    if (message != null) {
        org.junit.jupiter.api.assertNotNull(actual, message)
    } else {
        org.junit.jupiter.api.assertNotNull(actual)
    }

    return actual
}

/**
 * Inspired by the kotlin-test library
 */
fun assertTrue(message: String? = null, block: () -> Boolean) {
    org.junit.jupiter.api.Assertions.assertTrue(block(), message)
}
