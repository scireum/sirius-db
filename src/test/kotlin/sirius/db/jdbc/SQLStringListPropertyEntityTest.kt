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
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class SQLStringListPropertyEntityTest {
    @Test
    fun `test many list entries`() {
        val sqlStringListPropertyEntity = SQLStringListPropertyEntity()
        sqlStringListPropertyEntity.stringList.add("test1").add("test2").add("test3").add("test4")
        oma.update(sqlStringListPropertyEntity)
        val result = oma.find(SQLStringListPropertyEntity::class.java, sqlStringListPropertyEntity.getId()).get()

        assertEquals(4, result.stringList.size())
        assertTrue { result.stringList.contains("test1") }
        assertTrue { result.stringList.contains("test2") }
        assertTrue { result.stringList.contains("test3") }
        assertTrue { result.stringList.contains("test4") }
    }

    @Test
    fun `test one list entry`() {
        val sqlStringListPropertyEntity = SQLStringListPropertyEntity()
        sqlStringListPropertyEntity.stringList.add("test1")
        oma.update(sqlStringListPropertyEntity)
        val result = oma.find(SQLStringListPropertyEntity::class.java, sqlStringListPropertyEntity.getId()).get()

        assertEquals(1, result.stringList.size())
        assertTrue { result.stringList.contains("test1") }
    }

    @Test
    fun `test no list enties`() {
        val entity = SQLStringListPropertyEntity()
        oma.update(entity)
        val result = oma.find(SQLStringListPropertyEntity::class.java, entity.getId()).get()

        assertEquals(0, result.stringList.size())
    }

    @Test
    fun `test exception is thrown, if the list is to long for the field`() {
        val entity = SQLStringListPropertyEntity()
        entity.shortStringList.add("test1").add("test2").add("test3").add("test4")

        val exception = assertThrows<HandledException> { oma.update(entity) }
        assertEquals(
                "Der Wert 'test1,test2,test3,test4' im Feld 'Model.shortStringList' ist mit 23 Zeichen zu " +
                        "lang. Maximal sind 20 Zeichen erlaubt.", exception.message
        )
    }

    @Test
    fun `test exception is thrown for empty lists, if the list is non null-allowed`() {
        val entity = SQLStringListNonNullAllowedPropertyEntity()

        val exception = assertThrows<HandledException> { oma.update(entity) }
        assertFalse { exception.message!!.contains("'stringList' doesn't have a valault value") }
    }

    companion object {
        @Part
        lateinit var oma: OMA
    }
}
