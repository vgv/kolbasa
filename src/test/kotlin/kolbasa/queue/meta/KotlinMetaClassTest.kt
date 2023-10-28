package kolbasa.queue.meta

import kolbasa.queue.Searchable
import kolbasa.queue.Unique
import kolbasa.schema.Const
import org.junit.jupiter.api.Test
import kotlin.test.*

class KotlinMetaClassTest {

    @Test
    fun getFields() {
        val metaClass = KotlinMetaClass(TestData::class)

        assertEquals(3, metaClass.fields.size)

        metaClass.fields[0].let { field ->
            assertEquals("x", field.fieldName)
            assertEquals(Const.META_FIELD_NAME_PREFIX + "x", field.dbColumnName)
            assertEquals("int", field.dbColumnType)
            assertEquals(MetaIndexType.NO_INDEX, field.dbIndexType)
        }

        metaClass.fields[1].let { field ->
            assertEquals("y", field.fieldName)
            assertEquals(Const.META_FIELD_NAME_PREFIX + "y", field.dbColumnName)
            assertEquals("varchar(${Const.META_FIELD_STRING_TYPE_MAX_LENGTH})", field.dbColumnType)
            assertEquals(MetaIndexType.JUST_INDEX, field.dbIndexType)
        }

        metaClass.fields[2].let { field ->
            assertEquals("z", field.fieldName)
            assertEquals(Const.META_FIELD_NAME_PREFIX + "z", field.dbColumnName)
            assertEquals("boolean", field.dbColumnType)
            assertEquals(MetaIndexType.UNIQUE_INDEX, field.dbIndexType)
        }
    }

    @Test
    fun findMetaFieldByName() {
        val metaClass = KotlinMetaClass(TestData::class)

        assertSame(metaClass.fields[0], metaClass.findMetaFieldByName("x"))
        assertSame(metaClass.fields[1], metaClass.findMetaFieldByName("y"))
        assertSame(metaClass.fields[2], metaClass.findMetaFieldByName("z"))
    }

    @Test
    fun createInstance() {
        val metaClass = KotlinMetaClass(TestData::class)

        val expected = TestData(1, "one", true)
        val created = metaClass.createInstance(arrayOf(1, "one", true))

        assertNotSame(expected, created)
        assertEquals(expected, created)
    }

    @Test
    fun testErrorIfNotDataClass() {
        assertFailsWith<IllegalStateException> { KotlinMetaClass(TestNotData::class) }
    }

    @Test
    fun testErrorIfPrivateDataClass() {
        assertFailsWith<IllegalStateException> { KotlinMetaClass(PrivateTestData::class) }
    }
}

internal data class TestData(
    val x: Int,
    @Searchable
    val y: String,
    @Unique
    val z: Boolean
)

internal class TestNotData(
    val x: Int,
    val y: Int
)

private data class PrivateTestData(
    val x: Int,
    val y: Int
)