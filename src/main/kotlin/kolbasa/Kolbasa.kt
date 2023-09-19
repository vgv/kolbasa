package kolbasa

import kolbasa.stats.sql.SqlDumpConfig

object Kolbasa {

    @JvmStatic
    @Volatile
    var sweepConfig: SweepConfig = SweepConfig()

    @JvmStatic
    @Volatile
    var sqlDumpConfig: SqlDumpConfig = SqlDumpConfig()

}
