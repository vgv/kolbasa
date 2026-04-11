package kolbasa.cluster.butcher.config

import kolbasa.cluster.butcher.ButcherException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows

class PgEndpointTest {

    @Test
    fun testParsePgEndpoint_AllFields() {
        val endpoint = PgEndpoint.parsePgEndpoint(
            id = "node-01",
            libpqValues = mapOf(
                "host" to "db01.internal",
                "port" to "5433",
                "dbname" to "orders",
                "user" to "app",
                "password" to "secret",
                "schema" to "public",
            )
        )

        assertEquals("node-01", endpoint.id)
        assertEquals("db01.internal", endpoint.host)
        assertEquals(5433, endpoint.port)
        assertEquals("orders", endpoint.dbName)
        assertEquals("app", endpoint.user)
        assertEquals("secret", endpoint.password)
        assertEquals("public", endpoint.schema)
    }

    @Test
    fun testParsePgEndpoint_OnlyRequiredFields() {
        val endpoint = PgEndpoint.parsePgEndpoint(
            id = "node-01",
            libpqValues = mapOf("host" to "h", "dbname" to "db")
        )

        assertEquals("node-01", endpoint.id)
        assertEquals("h", endpoint.host)
        assertNull(endpoint.port)
        assertEquals("db", endpoint.dbName)
        assertNull(endpoint.user)
        assertNull(endpoint.password)
        assertNull(endpoint.schema)
    }

    @Test
    fun testParsePgEndpoint_MissingHost_Throws() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            PgEndpoint.parsePgEndpoint("node-01", mapOf("dbname" to "db"))
        }
        assertTrue(ex.messageToShow.contains("host"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("node-01"), ex.messageToShow)
    }

    @Test
    fun testParsePgEndpoint_MissingDbName_Throws() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            PgEndpoint.parsePgEndpoint("node-01", mapOf("host" to "db.host"))
        }
        assertTrue(ex.messageToShow.contains("dbname"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("node-01"), ex.messageToShow)
    }

    @Test
    fun testParsePgEndpoint_InvalidPort_Throws() {
        val ex = assertThrows<ButcherException.InvalidConfigurationException> {
            PgEndpoint.parsePgEndpoint(
                "node-01",
                mapOf("host" to "h", "dbname" to "db", "port" to "abc")
            )
        }
        assertTrue(ex.messageToShow.contains("abc"), ex.messageToShow)
        assertTrue(ex.messageToShow.contains("node-01"), ex.messageToShow)
    }

    @Test
    fun testParsePgEndpoint_BlankPort_Throws() {
        assertThrows<ButcherException.InvalidConfigurationException> {
            PgEndpoint.parsePgEndpoint(
                "node-01",
                mapOf("host" to "h", "dbname" to "db", "port" to "")
            )
        }
    }

    @Test
    fun testParsePgEndpoint_UnknownKeysIgnored() {
        val endpoint = PgEndpoint.parsePgEndpoint(
            "n",
            mapOf(
                "host" to "h",
                "dbname" to "db",
                "sslmode" to "require",
                "application_name" to "butcher"
            )
        )
        assertEquals("h", endpoint.host)
        assertEquals("db", endpoint.dbName)
        assertNull(endpoint.user)
    }

    @Test
    fun testToString_WithPort() {
        val endpoint = PgEndpoint(
            id = "node-01",
            host = "db.host",
            port = 5433,
            dbName = "orders",
            user = "app",
            password = "super-secret",
            schema = "public",
        )

        assertEquals("node-01 (db.host:5433/orders)", endpoint.toString())
    }

    @Test
    fun testToString_WithoutPort() {
        val endpoint = PgEndpoint(
            id = "node-01",
            host = "db.host",
            port = null,
            dbName = "orders",
            user = null,
            password = null,
            schema = null,
        )

        assertEquals("node-01 (db.host:5432/orders)", endpoint.toString())
    }
}
