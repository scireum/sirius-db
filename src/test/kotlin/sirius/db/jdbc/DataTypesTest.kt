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
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Amount
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class DataTypesTest {
    @Test
    fun `reading and writing long works`() {
        var test = DataTypesEntity()

        test.longValue = Long.MAX_VALUE

        oma.update(test)

        test = oma.refreshOrFail(test)

        assertEquals(Long.MAX_VALUE, test.longValue)
    }

    @Test
    fun `StringProperty detects changes to empty values properly`() {
        var test = DataTypesEntity()
        test.stringValueNoDefault = ""

        oma.update(test)
        val afterFirstSave = oma.refreshOrFail(test)

        afterFirstSave.stringValueNoDefault = ""
        assertFalse { afterFirstSave.isAnyMappingChanged }
    }

    @Test
    fun `default values work when using parseValue()`() {
        var test = DataTypesEntity()
        val longValue = test.descriptor.getProperty("longValue")
        val intValue = test.descriptor.getProperty("intValue")
        val stringValue = test.descriptor.getProperty("stringValue")
        val amountValue = test.descriptor.getProperty("amountValue")
        val boolValue = test.descriptor.getProperty("boolValue")
        val localTimeValue = test.descriptor.getProperty("localTimeValue")
        val localDateValue = test.descriptor.getProperty("localDateValue")
        val enumValue = test.descriptor.getProperty("enumValue")

        longValue.parseValue(test, Value.of(null))
        intValue.parseValue(test, Value.of(null))
        stringValue.parseValue(test, Value.of(null))
        amountValue.parseValue(test, Value.of(null))
        boolValue.parseValue(test, Value.of(null))
        localTimeValue.parseValue(test, Value.of(null))
        localDateValue.parseValue(test, Value.of(null))
        enumValue.parseValue(test, Value.of(null))

        oma.update(test)
        test = oma.refreshOrFail(test)

        val threeHundred = 300
        assertEquals(Amount.of(threeHundred), test.amountValue)
        assertEquals(100L, test.longValue)
        assertEquals(200, test.intValue)
        assertEquals("test", test.stringValue)
        assertTrue { test.boolValue }
        assertEquals(10, test.localTimeValue.hour)
        assertEquals(15, test.localTimeValue.minute)
        assertEquals(30, test.localTimeValue.second)
        assertEquals(2018, test.localDateValue.year)
        assertEquals(1, test.localDateValue.month.value)
        assertEquals(1, test.localDateValue.dayOfMonth)
        assertEquals(DataTypesEntity.TestEnum.Test2, test.testEnum)
    }

    @Test
    fun `default values work`() {

        var test = DataTypesEntity()

        oma.update(test)

        test = oma.refreshOrFail(test)

        val threeHundred = 300
        assertEquals(Amount.of(threeHundred), test.amountValue)
        assertEquals(100L, test.longValue)
        assertEquals(200, test.intValue)
        assertEquals("test", test.stringValue)
        assertTrue { test.boolValue }
        assertEquals(10, test.localTimeValue.hour)
        assertEquals(15, test.localTimeValue.minute)
        assertEquals(30, test.localTimeValue.second)
        assertEquals(2018, test.localDateValue.year)
        assertEquals(1, test.localDateValue.month.value)
        assertEquals(1, test.localDateValue.dayOfMonth)
        assertEquals(DataTypesEntity.TestEnum.Test2, test.testEnum)
    }

    @Test
    fun `determining the length of enums work`() {
        val test = DataTypesEntity()

        assertEquals(8, test.descriptor.getProperty("enumValue").length)
        assertEquals(10, test.descriptor.getProperty("enumValueFixedLength").length)
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        fun setupSpec() {
            oma.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
