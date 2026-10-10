package kolbasa.queue.meta

import kolbasa.queue.meta.InstantField.Companion.PG_TIMESTAMPTZ
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.postgresql.jdbc.TimestampUtils
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*

internal class InstantFieldFormatTest {

    private val pgjdbc = TimestampUtils(true) { TimeZone.getTimeZone("UTC") }

    private fun render(value: Instant): String =
        PG_TIMESTAMPTZ.format(OffsetDateTime.ofInstant(value, ZoneOffset.UTC))

    @Test
    fun testFormatter_regularYear() {
        val expected = "2026-09-12 06:15:30.4+00"
        val instant = Instant.parse("2026-09-12T10:15:30.400+04:00")

        assertEquals(expected, render(instant))
        assertEquals(expected, pgjdbc.toString(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)))
    }

    @Test
    fun testFormatter_bcYear() {
        val expected = "2026-09-12 06:15:30.4+00 BC"
        val instant = Instant.parse("-2025-09-12T10:15:30.400+04:00")

        assertEquals(expected, render(instant))
        assertEquals(expected, pgjdbc.toString(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)))
    }

    @Test
    fun testFormatter_yearBeyondFourDigits() {
        // default ISO-8601 formatter would render this as "+20260-09-12T10:15:30.400Z", which PostgreSQL rejects
        // with "time zone displacement out of range"
        // check that our formatter renders it as PostgreSQL expects, without leading + sign
        val expected = "20260-09-12 06:15:30.4+00"
        val instant = Instant.parse("+20260-09-12T10:15:30.400+04:00")

        assertEquals(expected, render(instant))
        assertEquals(expected, pgjdbc.toString(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)))
    }

    @Test
    fun testFormatter_yearPaddedWithLeadingZeros() {
        // year 2 AD: the formatter pads it to "0002", and that padding is load-bearing. With fewer than three digits
        // PostgreSQL stops reading the field as a year and falls back to the field order of the session's DateStyle,
        // so "2-01-01" becomes 2001-02-01 under the default MDY, 2001-01-02 under DMY, and 2002-01-01 under YMD.
        // Wrong three different ways, silently, depending on a client setting we do not control.
        // This is why we need to have leading zeros to convert year "2 AD" to "0002" in the formatter.
        val expected = "0002-09-12 06:15:30.4+00"
        val instant = Instant.parse("0002-09-12T10:15:30.400+04:00")

        assertEquals(expected, render(instant))
        assertEquals(expected, pgjdbc.toString(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)))
    }

}
