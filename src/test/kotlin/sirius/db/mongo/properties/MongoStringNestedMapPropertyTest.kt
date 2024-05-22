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
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class MongoStringNestedMapPropertyTest {
    @Test
    fun `reading and writing works`() {

        val mongoStringNestedMapEntity = MongoStringNestedMapEntity()
        val timestamp = LocalDateTime.now().minusDays(2)
        mongoStringNestedMapEntity.map.put(
                "X",
                MongoStringNestedMapEntity.NestedEntity().withValue1("Y").withValue2(timestamp)
        )
        mango.update(mongoStringNestedMapEntity)
        var resolved = mango.refreshOrFail(mongoStringNestedMapEntity)

        assertEquals(1, resolved.map.size())

        assertTrue { resolved.map.containsKey("X") }
        assertEquals("Y", resolved.map.get("X").get().value1)
        assertEquals(timestamp.truncatedTo(ChronoUnit.MILLIS), resolved.map.get("X").get().value2)

        resolved.map.modify()["X"]?.withValue1("ZZZ")
        mango.update(resolved)
        resolved = mango.refreshOrFail(mongoStringNestedMapEntity)

        assertEquals(1, resolved.map.size())
        assertTrue { resolved.map.containsKey("X") }
        assertEquals("ZZZ", resolved.map.get("X").get().value1)

        resolved.map.modify().remove("X")
        mango.update(resolved)
        resolved = mango.refreshOrFail(mongoStringNestedMapEntity)

        assertEquals(0, resolved.map.size())
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
