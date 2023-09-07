/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mongo.Mango
import sirius.db.mongo.Mongo
import sirius.db.mongo.ValidatedByTestEntity
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class ValidatedByTest {

    @Test
    fun `PropertyValidator blocks storing invalid value`() {
        // Store Test Entity to Mongo.
        val test = ValidatedByTestEntity()
        test.strictStringTest = "invalid"

        val warnings = mango.validate(test)
        assertEquals(1, warnings.size)
        assertEquals("Invalid value!", warnings[0])

        assertThrows<HandledException> {
            mango.update(test)
        }
    }

    @Test
    fun `PropertyValidator allows storing valid value`() {
        // Store Test Entity to Mongo.
        val test = ValidatedByTestEntity()
        test.strictStringTest = "valid"

        mango.update(test)
    }

    @Test
    fun `PropertyValidator in strict mode prevents updating an entity with invalid value already stored`() {
        // Store Test Entity to Mongo.
        var test = ValidatedByTestEntity()
        test.strictStringTest = "valid"
        mango.update(test)

        // Set the strict field to an invalid value directly in the database.
        mongo.update()
                .where(ValidatedByTestEntity.ID, test.id)
                .set(ValidatedByTestEntity.STRICT_STRING_TEST, "invalid")
                .executeForOne(ValidatedByTestEntity::class.java)

        // Retrieve it back from Mongo.
        test = mango.refreshOrFail(test)
        // Update an unrelated and unvalidated field.
        test.unvalidatedStringTest = "test"

        // We did not touch the strictly validated field, but the strict mode still prevents updating.
        val warnings = mango.validate(test)
        assertEquals(1, warnings.size)
        assertEquals("Invalid value!", warnings[0])

        assertThrows<HandledException> {
            mango.update(test)
        }
    }

    @Test
    fun `PropertyValidator without strict mode allows updating an entity with invalid value already stored`() {
        // Store Test Entity to Mongo.
        var test = ValidatedByTestEntity()
        test.lenientStringTest = "valid"
        mango.update(test)

        // Set the strict field to an invalid value directly in the database.
        mongo.update()
                .where(ValidatedByTestEntity.ID, test.id)
                .set(ValidatedByTestEntity.LENIENT_STRING_TEST, "invalid")
                .executeForOne(ValidatedByTestEntity::class.java)

        // Retrieve it back from Mongo.
        test = mango.refreshOrFail(test)
        // Update an unrelated and unvalidated field.
        test.unvalidatedStringTest = "test"

        // The warning for the leniently validated field should be returned.
        val warnings = mango.validate(test)
        assertEquals(1, warnings.size)
        assertEquals("Invalid value!", warnings[0])

        // But as we did not alter the leniently validated field, the update should be allowed.
        mango.update(test)
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
