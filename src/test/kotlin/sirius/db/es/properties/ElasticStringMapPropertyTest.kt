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

@ExtendWith(SiriusExtension::class)
class ElasticStringMapPropertyTest {
    @Test
    fun `reading and writing works`() {
        val esStringMapEntity = ESStringMapEntity()
        esStringMapEntity.map.put("Test", "1").put("Foo", "2")
        elastic.update(esStringMapEntity)
        var resolved = elastic.refreshOrFail(esStringMapEntity)

        assertEquals(2,resolved.map.size())
        assertEquals("1",resolved.map.get("Test").get())
        assertEquals("2", resolved.map.get("Foo").get())

        resolved.map.modify().remove("Test")
        elastic.update(resolved)
        resolved = elastic.refreshOrFail(esStringMapEntity)

        assertEquals(1,resolved.map.size())
        assertFalse { resolved.map.containsKey("Test") }
        assertEquals("2",resolved.map.get("Foo").get())
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
