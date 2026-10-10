package kolbasa.mutator

import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors

class MutatorSchemaHelpersTest {

    @Test
    fun testCalculateAsyncExecutor() {
        val mutatorExecutor = Executors.newCachedThreadPool()
        val defaultExecutor = Executors.newCachedThreadPool()

        assertSame(defaultExecutor, MutatorSchemaHelpers.calculateAsyncExecutor(null, defaultExecutor))
        assertSame(mutatorExecutor, MutatorSchemaHelpers.calculateAsyncExecutor(mutatorExecutor, defaultExecutor))
    }


}
