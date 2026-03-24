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
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@ExtendWith(SiriusExtension::class)
class MongoStringLocalDateTimeMapPropertyTest {
    @Test
    fun `reading and writing works`() {
        val test = MongoStringLocalDateTimeMapEntity()
        val now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS)
        test.map.put("Test", now).put("Foo", now)
        mango.update(test)
        var resolved = mango.refreshOrFail(test)

        assertEquals(2, resolved.map.size())
        assertEquals(now, resolved.map.get("Test").get())
        assertEquals(now, resolved.map.get("Foo").get())

        resolved.map.modify().remove("Test")
        mango.update(resolved)
        resolved = mango.refreshOrFail(test)

        assertEquals(1, resolved.map.size())
        assertFalse { resolved.map.containsKey("Test") }
        assertEquals(now, resolved.map.get("Foo").get())
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
