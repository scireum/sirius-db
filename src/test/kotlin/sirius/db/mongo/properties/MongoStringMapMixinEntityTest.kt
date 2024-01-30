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
class MongoStringMapMixinEntityTest {
    @Test
    fun `reading and writing works`() {
        val test = MongoStringMapMixinEntity()
        test.`as`(MongoStringMapMixin::class.java).mapInMixin.put("key1", "value1").put("key2", "value2")
        mango.update(test)
        val refreshed = mango.refreshOrFail(test)
        val mixinOfRefreshed = refreshed.`as`(MongoStringMapMixin::class.java)

        assertEquals(2, mixinOfRefreshed.mapInMixin.size())
        assertEquals("value1", mixinOfRefreshed.mapInMixin.get("key1").get())
        assertEquals("value2", mixinOfRefreshed.mapInMixin.get("key2").get())

        val queried = mango.find(MongoStringMapMixinEntity::class.java, test.id).get()
        val mixinOfQueried = queried.`as`(MongoStringMapMixin::class.java)

        assertEquals(2, mixinOfQueried.mapInMixin.size())
        assertEquals("value1", mixinOfQueried.mapInMixin.get("key1").get())
        assertEquals("value2", mixinOfQueried.mapInMixin.get("key2").get())
    }

    companion object {
        @Part
        private lateinit var mango: Mango
    }
}
