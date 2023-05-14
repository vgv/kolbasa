package kolbasa.utils

internal class IntBox(private var value: Int) {
    fun get() = value
    fun getAndIncrement() = value++
}

internal class LongBox(private var value: Long = 0) {
    fun get(): Long = value
    fun inc(delta: Int) {
        value += delta
    }
}
