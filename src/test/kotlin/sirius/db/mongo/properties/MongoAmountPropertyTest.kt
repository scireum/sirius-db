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
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class MongoAmountPropertyTest {
    @Test
    fun `read and write of amount fields works`() {
        val values = listOf(-3.77, Double.MAX_VALUE, 0.00001, -0.00001)
        for (value in values) {
            assertEquals(Amount.of(value), saveAndRead(Amount.of(value)))
        }
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
    }
}
