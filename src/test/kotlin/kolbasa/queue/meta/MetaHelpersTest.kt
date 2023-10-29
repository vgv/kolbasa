package kolbasa.queue.meta

import io.mockk.InternalPlatformDsl.toStr
import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kolbasa.queue.meta.MetaHelpers.enumerateTypes
import kolbasa.queue.meta.MetaHelpers.findEnumValueOfFunction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger

internal class MetaHelpersTest {

    enum class TestColor {
        RED, GREEN, YELLOW
    }

    @JvmRecord
    data class TestRecord(val x: Int, val y: String) {
        constructor(a: String, b: String) : this(a.length, b)
    }

    // Class to emulate classical Java bean
    class JavaBean {

        var x: Int
        var y: String

        constructor(a: String, b: String) {
            this.x = a.length
            this.y = b
        }

        constructor(a: Int) {
            this.x = a
            this.y = a.toStr()
        }

        // this is a 'default', desired constructor
        constructor(x: Int, y: String) {
            this.x = x
            this.y = y
        }

    }


    @Test
    fun testGenerateMetaColumnName() {
        assertEquals("meta_int_value", MetaHelpers.generateMetaColumnName("intValue"))
        assertEquals("meta_long_value", MetaHelpers.generateMetaColumnName("LongValue"))

        assertEquals("meta_very_very_long_field_name", MetaHelpers.generateMetaColumnName("veryVeryLongFieldName"))
    }

    @Test
    fun testFindEnumValueOfFunction() {
        // Real enum
        assertEquals(TestColor::valueOf, findEnumValueOfFunction(TestColor::class))

        // Any other class
        assertNull(findEnumValueOfFunction(MetaHelpersTest::class))
    }

    @Test
    fun testEnumerateTypes() {
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                String::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { string() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Byte::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { byte() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Short::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { short() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Int::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { int() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Long::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { long() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Double::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { double() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Float::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { float() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Boolean::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { boolean() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Char::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { char() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                BigDecimal::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { bigdecimal() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                BigInteger::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { biginteger() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
        run {
            val string = mockk<() -> Unit>(relaxed = true)
            val byte = mockk<() -> Unit>(relaxed = true)
            val short = mockk<() -> Unit>(relaxed = true)
            val int = mockk<() -> Unit>(relaxed = true)
            val long = mockk<() -> Unit>(relaxed = true)
            val double = mockk<() -> Unit>(relaxed = true)
            val float = mockk<() -> Unit>(relaxed = true)
            val boolean = mockk<() -> Unit>(relaxed = true)
            val char = mockk<() -> Unit>(relaxed = true)
            val bigdecimal = mockk<() -> Unit>(relaxed = true)
            val biginteger = mockk<() -> Unit>(relaxed = true)
            val enum = mockk<() -> Unit>(relaxed = true)

            // Call
            enumerateTypes(
                Enum::class,
                string,
                long,
                int,
                short,
                byte,
                boolean,
                double,
                float,
                char,
                biginteger,
                bigdecimal,
                enum
            )

            verify { enum() }
            confirmVerified(string, long, int, short, byte, boolean, double, float, char, biginteger, bigdecimal, enum)
        }
    }

    @Test
    fun testFindCanonicalRecordConstructor() {
        val allConstructors = TestRecord::class.java.constructors
        assertEquals(2, allConstructors.size)

        val canonicalConstructor = MetaHelpers.findCanonicalRecordConstructor(TestRecord::class.java)
        assertEquals(Int::class.java, canonicalConstructor.parameters[0].type)
        assertEquals(String::class.java, canonicalConstructor.parameters[1].type)
    }

    @Test
    fun testFindJavaBeanDefaultConstructor() {
        val allConstructors = JavaBean::class.java.constructors
        assertEquals(3, allConstructors.size)

        val defaultConstructor = MetaHelpers.findJavaBeanDefaultConstructor(JavaBean::class.java)
        assertEquals(Int::class.java, defaultConstructor.parameters[0].type)
        assertEquals(String::class.java, defaultConstructor.parameters[1].type)
    }
}
