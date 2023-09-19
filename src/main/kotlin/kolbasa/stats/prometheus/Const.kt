package kolbasa.stats.prometheus

internal object Const {

    const val MILLIS_1 = 0.001
    const val MILLIS_2 = MILLIS_1 * 2
    const val MILLIS_5 = MILLIS_1 * 5
    const val MILLIS_10 = MILLIS_1 * 10
    const val MILLIS_20 = MILLIS_1 * 20
    const val MILLIS_50 = MILLIS_1 * 50
    const val MILLIS_100 = MILLIS_1 * 100
    const val MILLIS_200 = MILLIS_1 * 200
    const val MILLIS_500 = MILLIS_1 * 500

    const val SECOND_1 = 1.0
    const val SECOND_2 = SECOND_1 * 2
    const val SECOND_5 = SECOND_1 * 5
    const val SECOND_10 = SECOND_1 * 10
    const val SECOND_20 = SECOND_1 * 20
    const val SECOND_50 = SECOND_1 * 50
    const val SECOND_100 = SECOND_1 * 100

    fun histogramBuckets() = doubleArrayOf(
        MILLIS_1, MILLIS_2, MILLIS_5, MILLIS_10, MILLIS_20, MILLIS_50, MILLIS_100, MILLIS_200, MILLIS_500,
        SECOND_1, SECOND_2, SECOND_5, SECOND_10, SECOND_20, SECOND_50, SECOND_100
    )

}