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
        val expected = mapOf("1" to "2", "3" to "4")
        val actual = Helpers.arrayToMap(arrayOf("1", "2", "3", "4"))

        assertEquals(expected, actual)
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

    @Test
    fun testUtf8ByteLength() {
        val testStrings = listOf(
            "",                                                                     // empty
            "Hello, World!",                                                        // ASCII (1 byte per char)
            "Привет, мир!",                                                         // Cyrillic (2 bytes per char)
            "مرحبا بالعالم",                                                        // Arabic (2 bytes per char)
            "你好世界",                                                              // CJK (3 bytes per char)
            "สวัสดีชาวโลก",                                                           // Thai (3 bytes per char)
            "\uD83D\uDE00\uD83D\uDE80\uD83C\uDF1F",                                 // Emoji 😀🚀🌟 (4 bytes per char)
            "\uD834\uDD1E\uD834\uDD2B",                                             // Musical symbols 𝄞𝄫 (4 bytes per char)
            "\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67\u200D\uD83D\uDC66",   // ZWJ family emoji 👨‍👩‍👧‍👦
            "àéîõü ñ ç ž š",                                                        // Latin with diacritics
            "Hi Мир 世界 \uD83D\uDE00",                                              // Mix of 1b, 2b, 3b, 4b
        )

        for (str in testStrings) {
            val expectedSize = str.toByteArray(Charsets.UTF_8).size
            assertEquals(expectedSize, Helpers.utf8ByteLength(str), "Failed for: $str")
        }
    }

}
