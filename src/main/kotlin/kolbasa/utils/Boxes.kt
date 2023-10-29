package kolbasa.utils

internal class LongBox(private var value: Long = 0) {
    fun get(): Long = value
    fun inc(delta: Int) {
        value += delta
    }
}
