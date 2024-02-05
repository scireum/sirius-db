/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.jdbc.schema.Schema
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part

import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull

@ExtendWith(SiriusExtension::class)
class StringPropertyTest {
    @Test
    fun `reading and writing clobs works`() {
        schema.readyFuture.await(Duration.ofSeconds(45))
        var testClobEntity = TestClobEntity()
        testClobEntity.largeValue = "This is a test"
        oma.update(testClobEntity)
        testClobEntity = oma.refreshOrFail(testClobEntity)

        assertEquals("This is a test", testClobEntity.largeValue)
    }

    @Test
    fun `modification annotations are applied correctly`() {
        schema.readyFuture.await(Duration.ofSeconds(45))
        var stringManipulationTestEntity = StringManipulationTestEntity()
        stringManipulationTestEntity.trimmed = " Test "
        stringManipulationTestEntity.lower = " TEST "
        stringManipulationTestEntity.upper = " test "
        stringManipulationTestEntity.trimmedLower = " TEST "
        stringManipulationTestEntity.trimmedUpper = " test "

        oma.update(stringManipulationTestEntity)
        stringManipulationTestEntity = oma.refreshOrFail(stringManipulationTestEntity)

        assertEquals("Test", stringManipulationTestEntity.trimmed)
        assertEquals(" test ", stringManipulationTestEntity.lower)
        assertEquals(" TEST ", stringManipulationTestEntity.upper)
        assertEquals("test", stringManipulationTestEntity.trimmedLower)
        assertEquals("TEST", stringManipulationTestEntity.trimmedUpper)
    }

    @Test
    fun `modification annotations handle null correctly`() {
        schema.readyFuture.await(Duration.ofSeconds(45))
        var stringManipulationTestEntity = StringManipulationTestEntity()

        oma.update(stringManipulationTestEntity)
        stringManipulationTestEntity = oma.refreshOrFail(stringManipulationTestEntity)

        assertNull(stringManipulationTestEntity.trimmed)
        assertNull(stringManipulationTestEntity.lower)
        assertNull(stringManipulationTestEntity.trimmedLower)
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        @Part
        private lateinit var schema: Schema
    }
}
