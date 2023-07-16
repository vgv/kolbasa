package kolbasa.queue.meta

import kolbasa.queue.Searchable
import kolbasa.queue.Unique
import kolbasa.schema.Const
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertSame

class JavaBeanMetaClassTest {

    @Test
    fun getFields() {
        val metaClass = JavaBeanMetaClass(TestBean::class.java)

        assertEquals(3, metaClass.fields.size)

        metaClass.fields[0].let { field ->
            assertEquals("x", field.fieldName)
            assertEquals(Const.META_FIELD_NAME_PREFIX + "x", field.dbColumnName)
            assertEquals("int", field.dbColumnType)
            //assertEquals(MetaIndexType.NO_INDEX, field.dbIndexType)
        }

        metaClass.fields[1].let { field ->
            assertEquals("y", field.fieldName)
            assertEquals(Const.META_FIELD_NAME_PREFIX + "y", field.dbColumnName)
            assertEquals("varchar(${Const.META_FIELD_STRING_TYPE_MAX_LENGTH})", field.dbColumnType)
            //assertEquals(MetaIndexType.JUST_INDEX, field.dbIndexType)
        }

        metaClass.fields[2].let { field ->
            assertEquals("z", field.fieldName)
            assertEquals(Const.META_FIELD_NAME_PREFIX + "z", field.dbColumnName)
            assertEquals("boolean", field.dbColumnType)
            //assertEquals(MetaIndexType.UNIQUE_INDEX, field.dbIndexType)
        }
    }

    @Test
    fun findMetaFieldByName() {
        val metaClass = JavaBeanMetaClass(TestBean::class.java)

        assertSame(metaClass.fields[0], metaClass.findMetaFieldByName("x"))
        assertSame(metaClass.fields[1], metaClass.findMetaFieldByName("y"))
        assertSame(metaClass.fields[2], metaClass.findMetaFieldByName("z"))
    }

    @Test
    fun createInstance() {
        val metaClass = JavaBeanMetaClass(TestBean::class.java)

        val expected = TestBean(1, "one", true)
        val created = metaClass.createInstance(arrayOf(1, "one", true))

        assertNotSame(expected, created)
        assertEquals(expected, created)
    }
}

internal class TestBean(
    var x: Int,
    @Searchable
    var y: String,
    @Unique
    var z: Boolean
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TestBean

        if (x != other.x) return false
        if (y != other.y) return false
        return z == other.z
    }

    override fun hashCode(): Int {
        var result = x
        result = 31 * result + y.hashCode()
        result = 31 * result + z.hashCode()
        return result
    }
}
