/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
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
        val sqlQuery =
            database.createQuery("SELECT * FROM test_a [:disabled WHERE a = \${filter}]").set("disabled", false)
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

    @Test
    fun `SQLQuery maps the values of every row to their column labels`() {
        val rows = databases["test"].createQuery(MULTI_ROW_QUERY).queryList()

        assertEquals(2, rows.size)
        assertEquals(listOf("firstColumn", "SecondColumn", "thirdColumn"), rows[0].fieldsList.map { it.first })
        assertEquals(listOf("firstColumn", "SecondColumn", "thirdColumn"), rows[1].fieldsList.map { it.first })
        assertEquals(1, rows[0].getValue("FIRSTCOLUMN").asInt(0))
        assertEquals("a", rows[0].getValue("secondcolumn").asString())
        assertNull(rows[0].getValue("thirdColumn").get())
        assertEquals(2, rows[1].getValue("FIRSTCOLUMN").asInt(0))
        assertEquals("b", rows[1].getValue("secondcolumn").asString())
        assertEquals("c", rows[1].getValue("thirdColumn").asString())
    }

    @Test
    fun `SQLQuery yields the same rows when executed repeatedly`() {
        val sqlQuery = databases["test"].createQuery(MULTI_ROW_QUERY)

        val firstRun = sqlQuery.queryList().map { row -> row.fieldsList.map { it.first to it.second } }
        val secondRun = sqlQuery.queryList().map { row -> row.fieldsList.map { it.first to it.second } }

        assertEquals(2, firstRun.size)
        assertEquals(firstRun, secondRun)
    }

    companion object {
        /**
         * Yields two rows with mixed case column labels and a NULL value in the first row only.
         */
        private const val MULTI_ROW_QUERY =
            "SELECT 1 AS firstColumn, 'a' AS SecondColumn, NULL AS thirdColumn UNION ALL SELECT 2, 'b', 'c'"

        @Part
        private lateinit var databases: Databases
    }
}
