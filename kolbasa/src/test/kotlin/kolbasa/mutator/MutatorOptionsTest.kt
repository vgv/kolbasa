package kolbasa.mutator

import io.mockk.mockk
import kolbasa.cluster.Cluster
import kolbasa.cluster.ClusterMutator
import kolbasa.mutator.connection.ConnectionAwareDatabaseMutator
import kolbasa.mutator.datasource.DatabaseMutator
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import javax.sql.DataSource

class MutatorOptionsTest {

    // Never called: the mutators only keep them
    private val dataSource = mockk<DataSource>()
    private val cluster = mockk<Cluster>()

    @Test
    fun testOptions() {
        val options = MutatorOptions(maxMutatedMessagesKeepInMemory = 10)

        assertSame(options, ConnectionAwareDatabaseMutator(options).options)
        assertSame(options, DatabaseMutator(dataSource, options).options)
        assertSame(options, ClusterMutator(cluster, options).options)
    }

    @Test
    fun testOptions_Default() {
        assertSame(MutatorOptions.DEFAULT, ConnectionAwareDatabaseMutator().options)
        assertSame(MutatorOptions.DEFAULT, DatabaseMutator(dataSource).options)
        assertSame(MutatorOptions.DEFAULT, ClusterMutator(cluster).options)
    }
}
