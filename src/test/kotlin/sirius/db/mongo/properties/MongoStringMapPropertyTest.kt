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

@ExtendWith(SiriusExtension::class)
class MongoStringMapPropertyTest {
    @Test
    fun `reading and writing works`() {
        val test = MongoStringMapEntity()
        test.map.put("Test", "1").put("Foo", "2")
        mango.update(test)
        var resolved = mango.refreshOrFail(test)

        assertEquals(2, resolved.map.size())
        assertEquals("1", resolved.map.get("Test").get())
        assertEquals("2", resolved.map.get("Foo").get())

        resolved.map.modify().remove("Test")
        mango.update(resolved)
        resolved = mango.refreshOrFail(test)

        assertEquals(1, resolved.map.size())
        assertFalse { resolved.map.containsKey("Test") }
        assertEquals("2", resolved.map.get("Foo").get())
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
