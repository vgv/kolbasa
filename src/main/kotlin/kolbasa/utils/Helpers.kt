package kolbasa.utils

import java.security.MessageDigest

internal object Helpers {

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


}
