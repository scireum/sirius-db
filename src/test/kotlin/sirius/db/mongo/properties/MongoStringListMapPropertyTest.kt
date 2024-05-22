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
class MongoStringListMapPropertyTest {
    @Test
    fun `reading and writing works`() {
        val mongoStringListMapEntity = MongoStringListMapEntity()
        mongoStringListMapEntity.map.add("Test", "1").add("Foo", "2").add("Test", "3")
        mango.update(mongoStringListMapEntity)
        var resolved = mango.refreshOrFail(mongoStringListMapEntity)

        assertEquals(2, resolved.map.size())

        assertTrue { resolved.map.contains("Test", "1") }
        assertTrue { resolved.map.contains("Test", "3") }
        assertTrue { resolved.map.contains("Foo", "2") }

        resolved.map.remove("Test", "1")
        mango.update(resolved)
        resolved = mango.refreshOrFail(mongoStringListMapEntity)

        assertEquals(2, resolved.map.size())

        assertFalse { resolved.map.contains("Test", "1") }
        assertTrue { resolved.map.contains("Test", "3") }
        assertTrue { resolved.map.contains("Foo", "2") }
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
