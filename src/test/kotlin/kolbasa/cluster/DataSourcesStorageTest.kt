package kolbasa.cluster

import kolbasa.AbstractPostgresqlTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DataSourcesStorageTest : AbstractPostgresqlTest() {

    @Test
    fun testUpdate() {
        val dataSources = listOf(dataSource, dataSourceFirstSchema, dataSourceSecondSchema)
        val dataSourceStorage = DataSourcesStorage({ dataSources })

        // Make first update and check dataSources didn't change
        dataSourceStorage.update()

        val readyToSend = dataSourceStorage.readyToSendDataSources
        val readyToReceive = dataSourceStorage.readyToReceiveDataSources
        assertEquals(dataSources.toSet(), readyToSend.values.toSet())
        assertEquals(dataSources.toSet(), readyToReceive.values.toSet())

        // Make second update, dataSources should be literally the same if nothing changed
        dataSourceStorage.update()
        assertSame(readyToSend, dataSourceStorage.readyToSendDataSources)
        assertSame(readyToReceive, dataSourceStorage.readyToReceiveDataSources)

        // Update node states, make storage update and check dataSources again
        Schema.updateNodeInfo(dataSourceFirstSchema, sendEnabled = false, receiveEnabled = true)
        Schema.updateNodeInfo(dataSourceSecondSchema, sendEnabled = true, receiveEnabled = false)

        dataSourceStorage.update()
        assertNotSame(readyToSend, dataSourceStorage.readyToSendDataSources)
        assertNotSame(readyToReceive, dataSourceStorage.readyToReceiveDataSources)

        assertEquals((dataSources - dataSourceFirstSchema).toSet(), dataSourceStorage.readyToSendDataSources.values.toSet())
        assertEquals((dataSources - dataSourceSecondSchema).toSet(), dataSourceStorage.readyToReceiveDataSources.values.toSet())
    }

}
