/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.Elastic
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class ElasticStringBooleanMapPropertyTest {
    @Test
    fun `read and write a string boolean map works`() {
        val test = ESStringBooleanMapEntity()
        test.map.put("a", true).put("b", false).put("c", true).put("d", false)
        elastic.update(test)
        var resolved = elastic.refreshOrFail(test)

        assertEquals(4, resolved.map.size())

        assertTrue { resolved.map.get("a").get() }
        assertFalse { resolved.map.get("b").get() }
        assertTrue { resolved.map.get("c").get() }
        assertFalse { resolved.map.get("d").get() }

        resolved.map.modify().remove("a")
        elastic.update(resolved)
        resolved = elastic.refreshOrFail(test)

        assertEquals(3, resolved.map.size())

        assertFalse { resolved.map.containsKey("a") }
        assertTrue { resolved.map.get("c").get() }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
