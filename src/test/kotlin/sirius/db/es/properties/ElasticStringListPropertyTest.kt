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
class ElasticStringListPropertyTest {
    @Test
    fun `reading and writing works for Elasticsearch`() {
        val test = ESStringListEntity()
        test.list.add("Test").add("Hello").add("World")
        elastic.update(test)
        var resolved = elastic.refreshOrFail(test)

        assertEquals(3, resolved.list.size())
        assertTrue { resolved.list.contains("Test") }
        assertTrue { resolved.list.contains("Hello") }
        assertTrue { resolved.list.contains("World") }

        resolved.list.modify().remove("World")
        elastic.update(resolved)
        resolved = elastic.refreshOrFail(test)

        assertEquals(2, resolved.list.size())
        assertTrue { resolved.list.contains("Test") }
        assertTrue { resolved.list.contains("Hello") }
        assertFalse { resolved.list.contains("World") }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
