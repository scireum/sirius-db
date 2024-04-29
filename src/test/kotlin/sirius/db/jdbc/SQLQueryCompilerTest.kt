/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.jdbc.constraints.SQLConstraint
import sirius.db.jdbc.constraints.SQLQueryCompiler
import sirius.db.mixing.Mapping
import sirius.db.mixing.Mixing
import sirius.db.mixing.Property
import sirius.db.mixing.query.QueryField
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import sirius.kernel.testutil.Reflections
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

@ExtendWith(SiriusExtension::class)
class SQLQueryCompilerTest {
    @Test
    fun `compiling '' works`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )
        assertNull(queryCompiler.compile())
    }

    @Test
    fun `compiling colon yields an empty constraint`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                ":",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertNull(queryCompiler.compile())
    }

    @Test
    fun `compiling '=' yields an empty constraint`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "=",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertNull(queryCompiler.compile())
    }

    @Test
    fun `compiling 'firstname' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "firstname:",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("firstname IS NULL", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'not firstname' yields a constraint`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "!firstname:",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("NOT(firstname IS NULL)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling '-firstname' yields a constraint`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "-firstname:",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("NOT(firstname IS NULL)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'firstname=X OR lastname=Y' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "firstname:X OR lastname:Y",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("(firstname = X OR lastname = Y)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'firstname=X AND lastname=Y' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "firstname:X AND lastname:Y",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("(firstname = X AND lastname = Y)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'not firstname=X OR lastname=Y' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "!firstname:X OR lastname:Y",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("(NOT(firstname = X) OR lastname = Y)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling '-firstname=X OR lastname=Y' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "-firstname:X OR lastname:Y",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("(NOT(firstname = X) OR lastname = Y)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'not (firstname=X OR lastname=Y)' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "!(firstname:X OR lastname:Y)",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("NOT((firstname = X OR lastname = Y))", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling '-(firstname=X OR lastname=Y)' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "-(firstname:X OR lastname:Y)",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )
        assertEquals("NOT((firstname = X OR lastname = Y))", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling '-X' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "-X",
                listOf(QueryField.eq(TestEntity.FIRSTNAME))
        )

        assertEquals("NOT(firstname = X)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'backslash -X backslash' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "\"-X\"",
                listOf(QueryField.eq(TestEntity.FIRSTNAME))
        )

        assertEquals("firstname = -X", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'Y-X' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "Y-X",
                listOf(QueryField.eq(TestEntity.FIRSTNAME))
        )

        assertEquals("firstname = Y-X", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'firstname=test' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "firstname:test",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("firstname = test", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'firstname=type=value-123' works`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "firstname:type:value-123",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("firstname = type:value-123", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'firstname=type(value-123)' works`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "firstname:type(value-123)",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("firstname = type(value-123)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'hello=world' does not treat hello as a field`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "hello:world",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("LOWER(firstname) LIKE '%hello:world%'", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'hello==world' does not treat hello as a field`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "hello::world",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals("LOWER(firstname) LIKE '%hello::world%'", queryCompiler.compile().toString())

    }

    @Test
    fun `compiling 'hello bigger than world' silently drops the operator as hello isn't a field`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "hello > world",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )

        assertEquals(
                "(LOWER(firstname) LIKE '%hello%' AND LOWER(firstname) LIKE '%world%')",
                queryCompiler.compile().toString()
        )
    }

    @Test
    fun `compiling 'parent_name=Test' compiles into a JOIN FETCH`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(SmartQueryTestChildEntity::class.java),
                "parent.name:Test",
                Collections.emptyList()
        )

        assertEquals("parent.name = Test", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'parent#unknownProperty=Test' reports an appropriate error`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(SmartQueryTestChildEntity::class.java),
                "parent.unknownProperty:Test",
                Collections.emptyList()
        )

        assertThrows<IllegalArgumentException> { queryCompiler.compile() }
    }

    @Test
    fun `customizing constraint compilation works`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "is:chat",
                listOf(QueryField.contains(TestEntity.FIRSTNAME))
        )
        let {
            @Override
            fun compileCustomField(field: String): SQLConstraint {
                return Reflections.callPrivateMethod(it, "parseOperation", Mapping.named(field)) as SQLConstraint
            }

            @Override
            fun compileValue(property: Property, value: Any): Any {
                return value
            }
        }

        assertNotEquals("is = chat", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling a field with OR in its name works`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "firstname: x orderNumber: 1",
                Collections.emptyList()
        )

        assertEquals("(firstname = x AND orderNumber = 1)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling a field with AND in its name works`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "firstname: x andx: 1",
                Collections.emptyList()
        )

        assertEquals("(firstname = x AND andx = 1)", queryCompiler.compile().toString())
    }

    @Test
    fun `compiling 'foo - bar' works as expected`() {
        val queryCompiler = SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity::class.java),
                "foo - bar",
                listOf((QueryField.eq(TestEntity.FIRSTNAME)))
        )

        assertEquals("(firstname = foo AND firstname = bar)", queryCompiler.compile().toString())
    }

    companion object {
        @Part
        private lateinit var mixing: Mixing
    }
}
