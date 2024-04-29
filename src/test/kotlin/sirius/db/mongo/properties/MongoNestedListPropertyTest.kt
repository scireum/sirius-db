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

@ExtendWith(SiriusExtension::class)
class MongoNestedListPropertyTest {
    @Test
    fun `reading, change tracking and writing works`() {
        val test = MongoNestedListEntity()
        test.list.add(MongoNestedListEntity.NestedEntity().withValue1("X").withValue2("Y"))
        mango.update(test)
        var resolved = mango.refreshOrFail(test)

        assertEquals(1, resolved.list.size())
        assertEquals("X", resolved.list.data()[0].value1)
        assertEquals("Y", resolved.list.data()[0].value2)

        resolved.list.modify().get(0).withValue1("Z")
        mango.update(resolved)
        resolved = mango.refreshOrFail(test)

        assertEquals(1, resolved.list.size())
        assertEquals("Z", resolved.list.data()[0].value1)
        assertEquals("Y", resolved.list.data()[0].value2)

        resolved.list.modify().removeAt(0)
        mango.update(resolved)
        resolved = mango.refreshOrFail(test)

        assertEquals(0, resolved.list.size())
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
