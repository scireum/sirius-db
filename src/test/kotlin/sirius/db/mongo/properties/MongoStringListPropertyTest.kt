/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mongo.Mango
import sirius.db.mongo.Mongo
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class MongoStringListPropertyTest {
    @Test
    fun `reading and writing works for MongoDB`() {
        val test = MongoStringListEntity()
        test.list.add("Test").add("Hello").add("World")
        mango.update(test)
        var resolved = mango.refreshOrFail(test)

        assertEquals(3, resolved.list.size())
        assertTrue { resolved.list.contains("Test") }
        assertTrue { resolved.list.contains("Hello") }
        assertTrue { resolved.list.contains("World") }

        mongo.update().pull(MongoStringListEntity.LIST, "Hello").executeFor(resolved)
        resolved = mango.refreshOrFail(test)

        assertEquals(2, resolved.list.size())
        assertTrue { resolved.list.contains("Test") }
        assertFalse { resolved.list.contains("Hello") }
        assertTrue { resolved.list.contains("World") }

        resolved.list.modify().remove("World")
        mango.update(resolved)
        resolved = mango.refreshOrFail(test)

        assertEquals(1, resolved.list.size())
        assertTrue { resolved.list.contains("Test") }
        assertFalse { resolved.list.contains("Hello") }
        assertFalse { resolved.list.contains("World") }

        mongo.update().addEachToSet(MongoStringListEntity.LIST, listOf("a", "b", "c", "Test")).executeFor(resolved)
        resolved = mango.refreshOrFail(test)

        assertEquals(4, resolved.list.size())
        assertTrue { resolved.list.contains("Test") }
        assertTrue { resolved.list.contains("a") }
        assertTrue { resolved.list.contains("b") }
        assertTrue { resolved.list.contains("c") }
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
