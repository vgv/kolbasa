package kolbasa.mutator

import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertSame

class MutatorSchemaHelpersTest {

    @Test
    fun testCalculateAsyncExecutor() {
        val mutatorExecutor = Executors.newCachedThreadPool()
        val defaultExecutor = Executors.newCachedThreadPool()

        assertSame(defaultExecutor, MutatorSchemaHelpers.calculateAsyncExecutor(null, defaultExecutor))
        assertSame(mutatorExecutor, MutatorSchemaHelpers.calculateAsyncExecutor(mutatorExecutor, defaultExecutor))
    }


}
