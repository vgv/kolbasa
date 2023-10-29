package kolbasa.utils

internal class BytesCounter {

    private var bytes: Long = 0

    fun get(): Long = bytes

    fun inc(delta: Int) {
        bytes += delta
    }
}
