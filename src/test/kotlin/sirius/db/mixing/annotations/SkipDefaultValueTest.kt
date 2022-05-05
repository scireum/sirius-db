/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mongo.Mango
import sirius.db.mongo.Mongo
import sirius.db.mongo.SkipDefaultTestEntity
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class SkipDefaultValueTest {

    @Test
    fun `SkipDefaultValues works as expected`() {
        // when:
        var test = SkipDefaultTestEntity()
        mango.update(test)
        //and:
        test = mango.find(SkipDefaultTestEntity::class.java, test.id).get()
        // then:
        assertEquals(null, test.stringTest)
        assertEquals(false, test.isBoolTest)
        assertEquals(0, test.listTest.size())
        assertEquals(0, test.mapTest.size())
        // and:
        // Only the id (and _id) is stored...
        assertEquals(
            2, mongo.find()
                .where(SkipDefaultTestEntity.ID, test.id)
                .singleIn(SkipDefaultTestEntity::class.java).get().underlyingObject.keys.size
        )

        //        when:
        test = SkipDefaultTestEntity()
        test.stringTest = "Hello"
        test.isBoolTest = true
        test.listTest.add("Item")
        test.mapTest.put("Key", "Value")
        mango.update(test)
        // and :
        test = mango.find(SkipDefaultTestEntity::class.java, test.id).get()
        // then :
        assertEquals("Hello", test.stringTest)
        assertEquals(true, test.isBoolTest)
        assertEquals(true, test.listTest.contains("Item"))
        assertEquals("Value", test.mapTest.get("Key").orElse(""))
        // and :
        // All fields are stored...
        assertEquals(
            6, mongo.find().where(SkipDefaultTestEntity.ID, test.id).singleIn(
                SkipDefaultTestEntity::class.java
            ).get().underlyingObject.keys.size
        )

        // when:
        test.stringTest = null
        test.isBoolTest = false
        test.listTest.clear()
        test.mapTest.clear()
        mango.update(test)
        // and :
        test = mango.find(SkipDefaultTestEntity::class.java, test.id).get()
        // then :
        assertEquals(null, test.stringTest)
        assertEquals(false, test.isBoolTest)
        assertEquals(0, test.listTest.size())
        assertEquals(0, test.mapTest.size())
        // and :
        // Only the id (and again _id) is stored...
        assertEquals(
            2, mongo.find().where(SkipDefaultTestEntity.ID, test.id)
                .singleIn(SkipDefaultTestEntity::class.java).get().underlyingObject.keys.size
        )

    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
