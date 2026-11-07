package kolbasa.utils

/**
 * Counts bytes of data that will be sent or received from the database.
 */
internal sealed interface BytesCounter {

    fun get(): Long

    fun addInt()

    fun addLong()

    fun addString(value: String)

    fun addByteArray(value: ByteArray)

    /**
     * Counts every byte. Strings are counted in UTF-8 bytes.
     */
    class Counting : BytesCounter {

        private var bytes: Long = 0

        override fun get(): Long = bytes

        override fun addInt() {
            bytes += 4
        }

        override fun addLong() {
            bytes += 8
        }

        override fun addString(value: String) {
            bytes += Helpers.utf8ByteLength(value)
        }

        override fun addByteArray(value: ByteArray) {
            bytes += value.size
        }
    }

    /**
     * Counts nothing: the metrics are off, and nobody needs the result
     */
    object None : BytesCounter {

        override fun get(): Long = 0

        override fun addInt() {}

        override fun addLong() {}

        override fun addString(value: String) {}

        override fun addByteArray(value: ByteArray) {}
    }
}
