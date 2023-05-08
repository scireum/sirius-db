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
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@ExtendWith(SiriusExtension::class)
class SkipDefaultValueTest {

    @Test
    fun `SkipDefaultValues fields with default values are not stored in Mongo`() {
        // Store Test Entity to Mongo.
        var test = SkipDefaultTestEntity()
        mango.update(test)

        // Retrieve it back from Mongo.
        test = mango.find(SkipDefaultTestEntity::class.java, test.id).get()

        // Fields should equal the default/null values.
        assertEquals(null, test.stringTest)
        assertEquals(false, test.isBoolTest)
        assertEquals(0, test.listTest.size())
        assertEquals(0, test.mapTest.size())
        assertEquals(100, test.integerWithDefault)

        // No fields are marked as changed
        assertFalse(test.isAnyMappingChanged)

        // Also, only the id (and _id) is persisted in Mongo.
        assertEquals(
                2, mongo.find()
                .where(SkipDefaultTestEntity.ID, test.id)
                .singleIn(SkipDefaultTestEntity::class.java).get().underlyingObject.keys.size
        )
    }

    @Test
    fun `SkipDefaultValues fields with custom values are stored in Mongo`() {
        // Store new Test Entity with values for all fields to Mongo.
        var test = SkipDefaultTestEntity()
        test.stringTest = "Hello"
        test.isBoolTest = true
        test.listTest.add("Item")
        test.mapTest.put("Key", "Value")
        test.integerWithDefault = 500
        mango.update(test)

        // Retrieve it back from Mongo.
        test = mango.find(SkipDefaultTestEntity::class.java, test.id).get()

        // Fields should equal the custom values.
        assertEquals("Hello", test.stringTest)
        assertEquals(true, test.isBoolTest)
        assertEquals(true, test.listTest.contains("Item"))
        assertEquals("Value", test.mapTest.get("Key").orElse(""))
        assertEquals(500, test.integerWithDefault)

        // No fields are marked as changed
        assertFalse(test.isAnyMappingChanged)

        // Also, all fields are persisted in Mongo.
        assertEquals(
                7, mongo.find().where(SkipDefaultTestEntity.ID, test.id).singleIn(
                SkipDefaultTestEntity::class.java
        ).get().underlyingObject.keys.size
        )
    }

    @Test
    fun `SkipDefaultValues fields are removed from Mongo when reset to default value`() {
        // Store new Test Entity with values for all fields to Mongo.
        var test = SkipDefaultTestEntity()
        test.stringTest = "Hello"
        test.isBoolTest = true
        test.listTest.add("Item")
        test.mapTest.put("Key", "Value")
        test.integerWithDefault = 500
        mango.update(test)

        // Retrieve it back from Mongo.
        test = mango.find(SkipDefaultTestEntity::class.java, test.id).get()

        // Reset fields to default values and store to Mongo.
        test.stringTest = null
        test.isBoolTest = false
        test.listTest.clear()
        test.mapTest.clear()
        test.integerWithDefault = 100
        mango.update(test)

        // Retrieve it back from Mongo.
        test = mango.find(SkipDefaultTestEntity::class.java, test.id).get()

        // Fields should equal the default/null values.
        assertEquals(null, test.stringTest)
        assertEquals(false, test.isBoolTest)
        assertEquals(0, test.listTest.size())
        assertEquals(0, test.mapTest.size())
        assertEquals(100, test.integerWithDefault)

        // No fields are marked as changed
        assertFalse(test.isAnyMappingChanged)

        // Also, only the id (and _id) is persisted in Mongo.
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
