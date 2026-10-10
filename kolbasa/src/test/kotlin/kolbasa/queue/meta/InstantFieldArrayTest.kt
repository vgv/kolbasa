package kolbasa.queue.meta

import kolbasa.AbstractPostgresqlTest
import kolbasa.utils.JdbcHelpers.useConnectionWithAutocommit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * [InstantField] values reach PostgreSQL as a `timestamptz[]` bind whenever more than one message is sent at once,
 * or when a `oneOf` filter is used. pgjdbc renders such an array by calling `toString()` on every element, and
 * [OffsetDateTime.toString] prefixes years above 9999 with a plus sign (`+31197-09-14T02:48:05.400Z`), which
 * PostgreSQL rejects with "time zone displacement out of range".
 *
 * [InstantFieldFormatTest] pins the rendering itself against pgjdbc without a database; this one is the round
 * trip – the values really go into a `timestamptz` column through `unnest` and really come back.
 */
internal class InstantFieldArrayTest : AbstractPostgresqlTest() {

    override fun generateTestData(): List<String> {
        return listOf("create table $TABLE(v timestamptz)")
    }

    private val field = MetaField.ofInstant("instant_field")

    @Test
    fun test_ArrayBindHandler() {
        val values = listOf(
            Instant.parse("2026-09-12T10:15:30Z"),
            Instant.parse("2026-09-12T10:15:30.400Z"),
            // Five-digit year: OffsetDateTime.toString() would emit a leading '+' here
            OffsetDateTime.of(31197, 9, 14, 2, 48, 5, 400_000_000, ZoneOffset.UTC).toInstant(),
            // The largest timestamptz PostgreSQL can store
            InstantField.MAX_TIMESTAMPTZ,
            // Year 2 AD: the formatter pads it to "0002", and that padding is load-bearing. With fewer than
            // three digits PostgreSQL stops reading the field as a year and falls back to the field order of
            // the session's DateStyle, so "2-01-01" becomes 2001-02-01 under the default MDY, 2001-01-02
            // under DMY, and 2002-01-01 under YMD. Wrong three different ways, silently, depending on a
            // client setting we do not control. This is why we need to have leading zeros to convert year "2 AD" to "0002"
            // in the formatter.
            OffsetDateTime.of(2, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
            // BC: ISO year 0 is 1 BC, so 44 BC is ISO year -43
            OffsetDateTime.of(-43, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
            // The floor InstantField accepts: below it pgjdbc silently substitutes -infinity
            InstantField.MIN_TIMESTAMPTZ
        )

        dataSource.useConnectionWithAutocommit { connection ->
            connection.prepareStatement("insert into $TABLE select * from unnest(?)").use { statement ->
                field.fillPreparedStatementForValues(statement, 1, values)
                statement.execute()
            }
        }

        val readBack = mutableListOf<Instant>()
        dataSource.useConnectionWithAutocommit { connection ->
            connection.prepareStatement("select v from $TABLE order by v").use { statement ->
                statement.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        readBack += resultSet.getObject(1, OffsetDateTime::class.java).toInstant()
                    }
                }
            }
        }

        assertEquals(values.sorted(), readBack)
    }

    private companion object {
        const val TABLE = "instant_field_array_test"
    }
}
