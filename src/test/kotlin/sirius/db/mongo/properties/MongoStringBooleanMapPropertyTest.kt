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
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class MongoStringBooleanMapPropertyTest {
    @Test
    fun `read and write a string boolean map works`() {
        val mongoStringBooleanMapEntity = MongoStringBooleanMapEntity()
        mongoStringBooleanMapEntity.map.put("a", true).put("b", false).put("c", true).put("d", false)
        mango.update(mongoStringBooleanMapEntity)
        var resolved = mango.refreshOrFail(mongoStringBooleanMapEntity)

        assertEquals(4, resolved.map.size())
        assertTrue { resolved.map.get("a").get() }
        assertFalse { resolved.map.get("b").get() }
        assertTrue { resolved.map.get("c").get() }
        assertFalse { resolved.map.get("d").get() }

        resolved.map.modify().remove("a")
        mango.update(resolved)
        resolved = mango.refreshOrFail(mongoStringBooleanMapEntity)

        assertEquals(3, resolved.map.size())
        assertFalse { resolved.map.containsKey("a") }
        assertTrue { resolved.map.get("c").get() }
    }

    companion object {
        @Part
        private lateinit var mango: Mango
    }
}
