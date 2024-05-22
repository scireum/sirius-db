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

@ExtendWith(SiriusExtension::class)
class MongoStringListMixinEntityTest {
    @Test
    fun `reading and writing works`() {
        val mongoStringListMixinEntity = MongoStringListMixinEntity()
        mongoStringListMixinEntity.`as`(MongoStringListMixin::class.java).listInMixin.add("hello").add("world")
        mango.update(mongoStringListMixinEntity)
        val refreshed = mango.refreshOrFail(mongoStringListMixinEntity)
        val mixinOfRefreshed = refreshed.`as`(MongoStringListMixin::class.java)

        assertEquals(2, mixinOfRefreshed.listInMixin.size())
        assertEquals("hello", mixinOfRefreshed.listInMixin.data()[0])
        assertEquals("world", mixinOfRefreshed.listInMixin.data()[1])

        val queried = mango.find(MongoStringListMixinEntity::class.java, mongoStringListMixinEntity.id).get()
        val mixinOfQueried = queried.`as`(MongoStringListMixin::class.java)

        assertEquals(2, mixinOfQueried.listInMixin.size())
        assertEquals("hello", mixinOfQueried.listInMixin.data()[0])
        assertEquals("world", mixinOfQueried.listInMixin.data()[1])
    }

    companion object {
        @Part
        private lateinit var mango: Mango
    }
}
