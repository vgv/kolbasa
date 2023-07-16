package kolbasa.queue.meta

import kolbasa.schema.Const
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertSame

class JavaRecordMetaClassTest {

    @Test
    fun getFields() {
        val metaClass = JavaRecordMetaClass(TestRecord::class.java)

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
        val metaClass = JavaRecordMetaClass(TestRecord::class.java)

        assertSame(metaClass.fields[0], metaClass.findMetaFieldByName("x"))
        assertSame(metaClass.fields[1], metaClass.findMetaFieldByName("y"))
        assertSame(metaClass.fields[2], metaClass.findMetaFieldByName("z"))
    }

    @Test
    fun createInstance() {
        val metaClass = JavaRecordMetaClass(TestRecord::class.java)

        val expected = TestRecord(1, "one", true)
        val created = metaClass.createInstance(arrayOf(1, "one", true))

        assertNotSame(expected, created)
        assertEquals(expected, created)
    }
}

@JvmRecord
internal data class TestRecord(val x: Int, val y: String, val z: Boolean)
