/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema

import org.junit.jupiter.api.Test
import java.sql.Types
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * Pins how the schema model classes decide equality, which [SchemaTool] relies on when comparing the current schema
 * with the target schema.
 */
class SchemaEqualityTest {

    @Test
    fun `tables are equal by name ignoring case`() {
        assertEquals(table("users"), table("users"))
        assertEquals(table("users"), table("USERS"))
        assertNotEquals(table("users"), table("orders"))
        assertEquals(table("users").hashCode(), table("USERS").hashCode())
    }

    @Test
    fun `tables ignore columns and keys when comparing`() {
        val withColumn = table("users")
        withColumn.columns.add(column("id"))

        assertEquals(table("users"), withColumn)
    }

    @Test
    fun `tables are never equal to null or other types`() {
        assertFalse(table("users").equals(null))
        assertFalse(table("users").equals("users"))
    }

    @Test
    fun `keys are equal by name ignoring case`() {
        assertEquals(key("idx_name"), key("idx_name"))
        assertEquals(key("idx_name"), key("IDX_NAME"))
        assertNotEquals(key("idx_name"), key("idx_other"))
        assertEquals(key("idx_name").hashCode(), key("IDX_NAME").hashCode())
    }

    @Test
    fun `keys ignore their columns and uniqueness when comparing`() {
        val unique = key("idx_name")
        unique.isUnique = true
        unique.addColumn(1, "name")

        assertEquals(key("idx_name"), unique)
    }

    @Test
    fun `removing a key from a table matches it by name`() {
        val table = table("users")
        table.keys.add(key("idx_name"))
        table.keys.add(key("idx_other"))

        assertTrue(table.keys.remove(key("IDX_NAME")))
        assertEquals(listOf("idx_other"), table.keys.map { it.name })
    }

    @Test
    fun `foreign keys are equal by name ignoring case`() {
        assertEquals(foreignKey("fk_user"), foreignKey("fk_user"))
        assertEquals(foreignKey("fk_user"), foreignKey("FK_USER"))
        assertNotEquals(foreignKey("fk_user"), foreignKey("fk_order"))
        assertEquals(foreignKey("fk_user").hashCode(), foreignKey("FK_USER").hashCode())
    }

    @Test
    fun `foreign keys ignore their target when comparing`() {
        val pointingElsewhere = foreignKey("fk_user")
        pointingElsewhere.foreignTable = "other"

        assertEquals(foreignKey("fk_user"), pointingElsewhere)
    }

    @Test
    fun `names differing only in case are found in hash based collections`() {
        assertTrue(table("İNDEX") in hashSetOf(table("index")))
        assertTrue(table("ΟΣ") in hashSetOf(table("οσ")))
        assertTrue(key("IDX_NAME") in hashSetOf(key("idx_name")))
        assertTrue(foreignKey("FK_USER") in hashSetOf(foreignKey("fk_user")))
        assertEquals(0, table(null).hashCode())
    }

    @Test
    fun `null and empty names are treated alike`() {
        assertEquals(table(null), table(""))
        assertEquals(table(null).hashCode(), table("").hashCode())
        assertEquals(key(null), key(""))
        assertEquals(key(null).hashCode(), key("").hashCode())
        assertEquals(foreignKey(null), foreignKey(""))
        assertEquals(foreignKey(null).hashCode(), foreignKey("").hashCode())
        assertEquals(column(null), column(""))
        assertEquals(column(null).hashCode(), column("").hashCode())
    }

    @Test
    fun `comparing with a nameless object does not fail`() {
        assertNotEquals(table("users"), table(null))
        assertNotEquals(table(null), table("users"))
        assertEquals(table(null), table(null))
        assertNotEquals(key("idx_name"), key(null))
        assertNotEquals(key(null), key("idx_name"))
        assertNotEquals(foreignKey("fk_user"), foreignKey(null))
        assertNotEquals(foreignKey(null), foreignKey("fk_user"))
    }

    @Test
    fun `columns are equal by name respecting case`() {
        assertEquals(column("name"), column("name"))
        assertNotEquals(column("name"), column("NAME"))
        assertNotEquals(column("name"), column("other"))
        assertEquals(column("name").hashCode(), column("name").hashCode())
    }

    @Test
    fun `columns ignore their type and length when comparing`() {
        val different = column("name")
        different.type = Types.INTEGER
        different.length = 42

        assertEquals(column("name"), different)
    }

    private fun table(name: String?) = Table().apply { this.name = name }

    private fun key(name: String?) = Key().apply { this.name = name }

    private fun foreignKey(name: String?) = ForeignKey().apply { this.name = name }

    private fun column(name: String?) = TableColumn().apply { this.name = name }
}
