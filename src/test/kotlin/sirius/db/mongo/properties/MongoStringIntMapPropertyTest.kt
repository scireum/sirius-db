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
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@ExtendWith(SiriusExtension::class)
class MongoStringIntMapPropertyTe {
    @Test
    fun `read and write a string int map works`() {
        val mongoStringIntMapEntity = MongoStringIntMapEntity()
        mongoStringIntMapEntity.map.put("Test", 1).put("Foo", 2).put("Test", 3)
        mango.update(mongoStringIntMapEntity)
        var resolved = mango.refreshOrFail(mongoStringIntMapEntity)

        assertEquals(2, resolved.map.size())
        assertEquals(3, resolved.map.get("Test").get())
        assertEquals(2, resolved.map.get("Foo").get())

        resolved.map.modify().remove("Test")
        mango.update(resolved)
        resolved = mango.refreshOrFail(mongoStringIntMapEntity)

        assertEquals(1, resolved.map.size())
        assertFalse { resolved.map.containsKey("Test") }
        assertEquals(2, resolved.map.get("Foo").get())
    }

    companion object {
        @Part
        private lateinit var mango: Mango
    }
}
