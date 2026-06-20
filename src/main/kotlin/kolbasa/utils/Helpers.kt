package kolbasa.utils

import java.security.MessageDigest

internal object Helpers {

    /**
     * Converts a flat `[key1, value1, key2, value2, …]` array into a `Map` [`key1` → `value1`, `key2` → `value2`, …].
     *
     * @throws IllegalArgumentException if [array] has an odd number of elements, since it
     * cannot be split into complete key/value pairs.
     */
    fun arrayToMap(array: Array<String>?): Map<String, String>? {
        if (array == null) {
            return null
        }

        require(array.size % 2 == 0) {
            "Can't convert array with odd number of elements into map. Array: ${array.contentToString()}"
        }

        val data = mutableMapOf<String, String>()
        var i = 0
        while (i < array.size) {
            val key = array[i]
            val value = array[i + 1]
            data[key] = value
            i += 2
        }

        return data
    }

    /**
     * This implementation is far from being the most efficient one, but Kolbasa doesn't use MD5 hashing heavily,
     * so this should be sufficient for now.
     */
    fun md5Hash(input: String): String {
        val md = MessageDigest.getInstance("MD5")
        val hashBytes = md.digest(input.toByteArray(charset = Charsets.UTF_8))
        return hashBytes.joinToString("") { String.format("%02x", it) }
    }

    /**
     * Returns the first 10 characters of the MD5 hash of the input string.
     */
    fun shortHash(input: String): String {
        return md5Hash(input).take(10)
    }

    /**
     * Generates a random string of the specified length using characters from the specified alphabet.
     */
    fun randomString(length: Int, alphabet: String): String {
        val sb = StringBuilder(length)
        (1..length).forEach { _ ->
            sb.append(alphabet.random())
        }
        return sb.toString()
    }

    /**
     * Returns the number of bytes [value] would occupy when encoded as UTF-8.
     *
     * Equivalent to `value.toByteArray(Charsets.UTF_8).size`, but computes the length by
     * inspecting characters in place instead of allocating the encoded byte array — useful
     * for cheaply estimating payload sizes (see [BytesCounter]).
     *
     * Each UTF-16 code unit is mapped to its UTF-8 width: 1 byte for U+0000–U+007F,
     * 2 for U+0080–U+07FF, 3 for the rest of the BMP, and 4 for a surrogate pair
     * (the low surrogate is consumed together with its high surrogate).
     */
    fun utf8ByteLength(value: String): Int {
        var count = 0
        var i = 0
        while (i < value.length) {
            val ch = value[i]
            when {
                ch.code <= 0x7F -> count += 1         // ASCII
                ch.code <= 0x7FF -> count += 2        // 2-byte
                ch.isHighSurrogate() -> {
                    count += 4                        // surrogate pair → 4 bytes
                    i++                               // skip low surrogate
                }

                else -> count += 3                    // BMP (3-byte)
            }
            i++
        }
        return count
    }

}
