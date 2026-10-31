package kolbasa.mutator

import kolbasa.assertSameMethods
import kolbasa.mutator.connection.ConnectionAwareMutator
import kolbasa.mutator.datasource.Mutator
import org.junit.jupiter.api.Test

class DataSourceVsConnectionAwareMutatorsTest {

    @Test
    fun testSameMethods() {
        assertSameMethods(Mutator::class.java, ConnectionAwareMutator::class.java)
    }
}
