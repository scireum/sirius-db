/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.database.jdatabasec

import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.jdbc.Databases
import sirius.db.jdbc.Row
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Limit
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class JDBCTest {

    @Order(1)
    @Test
    fun `test database is loaded from config while profile is applied`() {
        val database = databases["test"]
        assertEquals(database.createQuery("SELECT 1").queryList().size, 1)
    }

    @Order(2)
    @Test
    fun `create table for 'test_a' works`() {
        val database = databases["test"]
        database.createQuery("CREATE TABLE test_a(a CHAR(10), b INT DEFAULT 1)").executeUpdate()
        assertEquals(database.createQuery("SELECT * FROM test_a").queryList().size, 0)
    }

    @Order(3)
    @Test
    fun `insert works on test table 'test_a'`() {
        val database = databases["test"]
        database.insertRow("test_a", mapOf("a" to "Hello"))
        database.insertRow("test_a", mapOf("a" to "Test", "b" to 2))
        assertEquals(database.createQuery("SELECT * FROM test_a").queryList().size, 2)
        assertEquals(
                database.createQuery("SELECT * FROM test_a ORDER BY a ASC").queryFirst()!!.getValue("A").asString(),
                "Hello"
        )
    }

    @Test
    fun `an IllegalArgumentException is created if an unknown column is selected`() {
        val r = Row()
        assertFailsWith<IllegalArgumentException>(block = {
            r.getValue("X")
        })
    }

    @Test
    fun `queryList returns all inserted rows`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT * FROM test_a")
        assertEquals(sqlQuery.queryList().size, 2)
    }

    @Test
    fun `SQLQuery#queryFirst returns a row`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT * FROM test_a ORDER BY a ASC")
        assertEquals(sqlQuery.queryFirst()!!.getValue("a").asString(), "Hello")
    }

    @Order(4)
    @Test
    fun `SQLQuery queryFirst returns null for an empty result set`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT a,b FROM test_a WHERE a = 'xxx'")
        assertNull(sqlQuery.queryFirst())
    }

    @Order(5)
    @Test
    fun `SQLQuery first returns an empty optional`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT * FROM test_a WHERE a = 'xxx'")
        assertTrue(sqlQuery.first().isEmpty)
    }

    @Order(6)
    @Test
    fun `SQLQuery executeUpdate works changes a row`() {
        val database = databases["test"]
        val numberOfRowsChanged = database.createQuery("UPDATE test_a SET a = 'xxx' WHERE a = 'Test'").executeUpdate()
        assertEquals(numberOfRowsChanged, 1)
        assertTrue(database.createQuery("SELECT * FROM test_a WHERE a = 'xxx'").first().isPresent)
    }

    @Test
    fun `the statement compiler omits an empty clause`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT * FROM test_a [WHERE a = \${filter}]").set("filter", null)
        assertEquals(sqlQuery.queryList().size, 2)
    }

    @Test
    fun `the statement compiler includes an non empty clause`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT * FROM test_a [WHERE a = \${filter}]").set("filter", "Hello")
        assertEquals(sqlQuery.queryList().size, 1)
    }

    @Test
    fun `the statement compiler omits a conditional clause`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT * FROM test_a [:disabled WHERE a = \${filter}]").set("disabled", false)
                .set("filter", null)
        assertEquals(sqlQuery.queryList().size, 2)
    }

    @Test
    fun `the statement compiler includes an active conditional clause`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT * FROM test_a [:enabled WHERE a = \${filter}]").set("enabled", true)
                .set("filter", "Hello")
        assertEquals(sqlQuery.queryList().size, 1)
    }

    @Test
    fun `the statement compiler expands hash fields correctly`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT * FROM test_a WHERE LOWER(a) LIKE #{filter}").set("filter", "HEL")
        assertEquals(sqlQuery.queryList().size, 1)
    }

    @Test
    fun `SQLQuery#iterate is evaluated correctly`() {
        val database = databases["test"]
        val sqlQuery = database.createQuery("SELECT a,b FROM test_a")
        sqlQuery.iterate({ row -> row.fieldsList.size == 2 }, Limit.UNLIMITED)
    }

    companion object {
        @Part
        private lateinit var databases: Databases
    }
}
