/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Json
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class LowLevelClientTest {
    @Test
    fun `create index works`() {
        val obj = elastic.getLowLevelClient().createIndex("test", 1, 1, null)

        assertTrue { obj.get("acknowledged").booleanValue() }
    }

    @Test
    fun `error handling works`() {
        assertThrows<HandledException> {
            elastic.getLowLevelClient().createIndex("invalid", 0, 1, null)
        }
    }

    @Test
    fun `index, get and delete works`() {
        elastic.getLowLevelClient().createIndex("test1", 1, 1, null)
        elastic.getLowLevelClient().index("test1", "TEST", null, null, null, Json.createObject().put("Hello", "World"))
        var data = elastic.getLowLevelClient().get("test1", "TEST", null, true)

        assertTrue { data.get("found").booleanValue() }
        assertEquals("World", data.get("_source").get("Hello").asText())

        elastic.getLowLevelClient().delete("test1", "TEST", null, null, null)
        data = elastic.getLowLevelClient().get("test1", "TEST", null, true)

        assertFalse { data.get("found").booleanValue() }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
