package kolbasa.stats.prometheus.metrics

internal object Const {

    private const val MILLIS_1 = 0.001
    private const val MILLIS_2 = MILLIS_1 * 2
    private const val MILLIS_5 = MILLIS_1 * 5
    private const val MILLIS_10 = MILLIS_1 * 10
    private const val MILLIS_20 = MILLIS_1 * 20
    private const val MILLIS_50 = MILLIS_1 * 50
    private const val MILLIS_100 = MILLIS_1 * 100
    private const val MILLIS_200 = MILLIS_1 * 200
    private const val MILLIS_500 = MILLIS_1 * 500

    private const val SECOND_1 = 1.0
    private const val SECOND_2 = SECOND_1 * 2
    private const val SECOND_5 = SECOND_1 * 5
    private const val SECOND_10 = SECOND_1 * 10
    private const val SECOND_20 = SECOND_1 * 20
    private const val SECOND_50 = SECOND_1 * 50
    private const val SECOND_100 = SECOND_1 * 100

    fun histogramBuckets() = doubleArrayOf(
        MILLIS_1, MILLIS_2, MILLIS_5, MILLIS_10, MILLIS_20, MILLIS_50, MILLIS_100, MILLIS_200, MILLIS_500,
        SECOND_1, SECOND_2, SECOND_5, SECOND_10, SECOND_20, SECOND_50, SECOND_100
    )

}
