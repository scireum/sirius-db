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
import sirius.kernel.commons.Tuple
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class MongoMultiPointTest {
    @Test
    fun `read and write multipoint works`() {
        var mongoMultiPointEntity = MongoMultiPointEntity()

        mango.update(mongoMultiPointEntity)

        mongoMultiPointEntity.locations.isEmpty

        val coords = listOf(Tuple.create(48.81734, 9.376294), Tuple.create(48.823356, 9.424718))
        mongoMultiPointEntity.locations.addAll(coords)
        mango.update(mongoMultiPointEntity)

        assertEquals(2, mongoMultiPointEntity.locations.size())

        mongoMultiPointEntity = mango.refreshOrFail(mongoMultiPointEntity)

        assertEquals(2, mongoMultiPointEntity.locations.size())
    }

    companion object {
        @Part
        private lateinit var mango: Mango
    }
}
