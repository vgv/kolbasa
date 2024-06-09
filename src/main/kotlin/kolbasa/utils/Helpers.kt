package kolbasa.utils

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

}
