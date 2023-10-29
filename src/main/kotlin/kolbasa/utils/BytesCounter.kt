package kolbasa.utils

/**
 * Counts bytes of data that will be sent or received from the database.
 *
 * @param precise if true, then the size of the string will be calculated in
 * bytes (much slower, but precise), otherwise in characters (very fast, but not so precise)
 */
internal class BytesCounter(private val precise: Boolean) {

    private var bytes: Long = 0

    fun get(): Long = bytes

    fun addInt() {
        bytes += 4
    }

    fun addLong() {
        bytes += 8
    }

    fun addString(value: String) {
        bytes += if (precise) {
            value.toByteArray().size
        } else {
            value.length
        }
    }

    fun addByteArray(value: ByteArray) {
        bytes += value.size
    }

}
