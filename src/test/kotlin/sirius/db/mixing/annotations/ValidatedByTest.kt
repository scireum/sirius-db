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
        test.stringTest = "invalid"

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
        test.stringTest = "valid"

        mango.update(test)
    }

    companion object {
        @Part
        private lateinit var mango: Mango
    }
}
