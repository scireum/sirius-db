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
import sirius.kernel.commons.Amount
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@ExtendWith(SiriusExtension::class)
class MongoAmountPropertyTest {
    @Test
    fun `read and write of amount fields works`() {
        val values = listOf(-3.77, Double.MAX_VALUE, 0.00001, -0.00001)
        for (value in values) {
            assertEquals(Amount.of(value), saveAndRead(Amount.of(value)))
            assertEquals(Amount.of(value), saveAndReadUsingPropertyParsing(Value.of(value)))
        }
    }

    @Test
    fun `property autoscaling of amount fields works`() {
        var test = MongoAmountEntity()
        val unscaledValue = Value.of("1,2345")

        val scaledAmountProperty = test.descriptor.getProperty("scaledAmount")
        scaledAmountProperty.parseValue(test, unscaledValue)
        val testAmountProperty = test.descriptor.getProperty("testAmount")
        testAmountProperty.parseValue(test, unscaledValue)
        mango.update(test)
        test = mango.refreshOrFail(test)
        val expectedAmount = unscaledValue.amount.round(MongoAmountEntity.AMOUNT_SCALE, RoundingMode.HALF_UP)
        assertEquals(expectedAmount, test.scaledAmount)
        assertEquals(unscaledValue.amount, test.testAmount)

        // Storing the same value twice must not trigger a change
        scaledAmountProperty.parseValue(test, unscaledValue)
        testAmountProperty.parseValue(test, unscaledValue)
        assertFalse { test.isAnyMappingChanged }
    }

    @Test
    fun `Persisting large scale amounts works properly`() {
        var test = MongoAmountEntity()
        val unscaledValue = Amount.ofRounded(BigDecimal("1.23456789"))

        test.scaledAmount = unscaledValue
        test.testAmount = Amount.ONE
        mango.update(test)
        test = mango.refreshOrFail(test)
        val expectedAmount = unscaledValue.round(MongoAmountEntity.AMOUNT_SCALE, RoundingMode.HALF_UP)
        assertEquals(expectedAmount, test.scaledAmount)
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        private fun saveAndRead(value: Amount): Amount {
            var mongoAmountEntity = MongoAmountEntity()
            mongoAmountEntity.testAmount = value
            mango.update(mongoAmountEntity)
            mongoAmountEntity = mango.refreshOrFail(mongoAmountEntity)
            return mongoAmountEntity.testAmount
        }

        private fun saveAndReadUsingPropertyParsing(value: Value): Amount {
            var mongoAmountEntity = MongoAmountEntity()
            val amountProperty = mongoAmountEntity.descriptor.getProperty("testAmount")
            amountProperty.parseValueFromImport(mongoAmountEntity, value)
            mango.update(mongoAmountEntity)
            mongoAmountEntity = mango.refreshOrFail(mongoAmountEntity)
            return mongoAmountEntity.testAmount
        }
    }
}
