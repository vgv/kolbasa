package kolbasa.cluster

import java.util.*
import javax.sql.DataSource

class DataSourcesStorage(private val dataSourcesProvider: () -> List<DataSource>) {

    @Volatile
    private var allKnownDataSources: SortedMap<NodeInfo, DataSource> = sortedMapOf()

    @Volatile
    internal var readyToSendDataSources: SortedMap<NodeInfo, DataSource> = sortedMapOf()

    @Volatile
    internal var readyToReceiveDataSources: SortedMap<NodeInfo, DataSource> = sortedMapOf()

    @Synchronized
    internal fun update() {
        val newDataSources = dataSourcesProvider().associateBy { dataSource ->
            Schema.createAndInitNodeTable(dataSource)
            requireNotNull(Schema.readNodeInfo(dataSource))
        }.toSortedMap()

        // if anything changed – update the state
        if (newDataSources != allKnownDataSources) {
            allKnownDataSources = newDataSources

            readyToSendDataSources = allKnownDataSources.filter { (key, _) ->
                key.sendEnabled
            }.toSortedMap()

            readyToReceiveDataSources = allKnownDataSources.filter { (key, _) ->
                key.receiveEnabled
            }.toSortedMap()
        }
    }
}
