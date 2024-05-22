/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mixing.Mixing
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

@ExtendWith(SiriusExtension::class)
class MongoQueryCompilerTest {
    @Test
    fun `listField generates an isEmptyListConstraint`() {
        val testEntity = MangoTestEntity()
        testEntity.firstname = "Compiler"
        testEntity.lastname = "Test"
        mango.update(testEntity)

        val queryResult = mango.select(
                MangoTestEntity::class.java
        ).where(
                QueryBuilder.FILTERS.queryString(
                        mixing.getDescriptor(
                                MangoTestEntity::class.java
                        ),
                        "id:${testEntity.getIdAsString()} superPowers:-"
                )
        ).queryFirst()

        assertNotNull(queryResult)
        assertEquals(testEntity.id, queryResult.id)
        assertFalse {
            mango.select(
                    MangoTestEntity::class.java
            ).where(
                    QueryBuilder.FILTERS.queryString(
                            mixing.getDescriptor(
                                    MangoTestEntity::class.java
                            ),
                            "id:${testEntity.getIdAsString()} superPowers:WriteCode"
                    )
            ).exists()
        }
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mixing: Mixing
    }
}
