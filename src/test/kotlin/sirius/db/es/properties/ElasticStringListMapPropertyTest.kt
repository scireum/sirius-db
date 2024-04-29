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
class ElasticStringListMapPropertyTest {
    @Test
    fun `reading and writing works`() {
        val test = ESStringListMapEntity()
        test.map.add("Test", "1").add("Foo", "2").add("Test", "3")
        elastic.update(test)
        var resolved = elastic.refreshOrFail(test)

        assertEquals(2, resolved.map.size())
        assertTrue { resolved.map.contains("Test", "1") }
        assertTrue { resolved.map.contains("Test", "3") }
        assertTrue { resolved.map.contains("Foo", "2") }

        resolved.map.remove("Test", "1")
        elastic.update(resolved)
        resolved = elastic.refreshOrFail(test)

        assertEquals(2, resolved.map.size())
        assertFalse { resolved.map.contains("Test", "1") }
        assertTrue { resolved.map.contains("Test", "3") }
        assertTrue { resolved.map.contains("Foo", "2") }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
