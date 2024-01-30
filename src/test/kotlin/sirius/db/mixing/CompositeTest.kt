/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mongo.Mango
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part

@ExtendWith(SiriusExtension::class)
class CompositeTest {
    @Test
    fun `Updating Composite inside Mixable works on Mongo`() {
        val refMongoEntity = RefMongoEntity()
        refMongoEntity.`as`(MongoMixable::class.java).composite.map.modify()["test"] = "data"

        assertDoesNotThrow { mango.update(refMongoEntity) }
    }

    companion object {
        @Part
        private lateinit var mango: Mango
    }
}
