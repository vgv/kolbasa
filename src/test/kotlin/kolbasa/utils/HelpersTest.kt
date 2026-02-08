package kolbasa.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows

class HelpersTest {

    @Test
    fun testArrayToMap_IfNull() {
        assertNull(Helpers.arrayToMap(null))
    }

    @Test
    fun testArrayToMap_IfEven() {
        assertEquals(mapOf("1" to "2", "3" to "4"), Helpers.arrayToMap(arrayOf("1", "2", "3", "4")))
    }

    @Test
    fun testArrayToMap_IfOdd() {
        assertThrows<IllegalArgumentException> {
            Helpers.arrayToMap(arrayOf("1", "2", "3", "4", "5"))
        }
    }

    @Test
    fun testMd5Hash() {
        val input = "test input"
        val expectedHash = "5eed650258ee02f6a77c87b748b764ec" // Precomputed MD5 hash of "test input"
        val actualHash = Helpers.md5Hash(input)
        assertEquals(expectedHash, actualHash)
    }

    @Test
    fun testShortHash() {
        val input = "test input"
        val expectedHash = "5eed650258" // Precomputed MD5 hash of "test input" is 5eed650258ee02f6a77c87b748b764ec
        val actualHash = Helpers.shortHash(input)
        assertEquals(expectedHash, actualHash)
    }

    @Test
    fun testRandomString() {
        val length = 10
        val alphabet = "abc"

        val randomStr = Helpers.randomString(length, alphabet)
        assertEquals(length, randomStr.length)
        assert(randomStr.all { it in alphabet })
    }

}
