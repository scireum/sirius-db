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
import sirius.db.mongo.Mongo
import sirius.db.mongo.QueryBuilder
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Strings
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class MongoFilledTest {
    @Test
    fun `filled notFilled query works`() {

        val fieldFilled = MongoFilledEntity()
        fieldFilled.testField = "test"
        val fieldNotFilled = MongoFilledEntity()

        mango.update(fieldFilled)
        mango.update(fieldNotFilled)

        assertEquals(
                fieldNotFilled.getIdAsString(), mango.select(MongoFilledEntity::class.java)
                .eq(MongoFilledEntity.TEST_FIELD, null)
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoFilledEntity::class.java)
                .eq(MongoFilledEntity.TEST_FIELD, null)
                .count()
        )

        assertEquals(
                fieldFilled.getIdAsString(), mango.select(MongoFilledEntity::class.java)
                .ne(MongoFilledEntity.TEST_FIELD, null)
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoFilledEntity::class.java)
                .ne(MongoFilledEntity.TEST_FIELD, null)
                .count()
        )

        assertEquals(
                fieldFilled.getIdAsString(), mango.select(MongoFilledEntity::class.java)
                .where(QueryBuilder.FILTERS.filled(MongoFilledEntity.TEST_FIELD))
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoFilledEntity::class.java)
                .where(QueryBuilder.FILTERS.filled(MongoFilledEntity.TEST_FIELD))
                .count()
        )

        assertEquals(
                fieldNotFilled.getIdAsString(), mango.select(MongoFilledEntity::class.java)
                .where(QueryBuilder.FILTERS.notFilled(MongoFilledEntity.TEST_FIELD))
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoFilledEntity::class.java)
                .where(QueryBuilder.FILTERS.notFilled(MongoFilledEntity.TEST_FIELD))
                .count()
        )

        assertTrue {
            mango.select(MongoFilledEntity::class.java).where(
                    QueryBuilder.FILTERS
                            .exists(MongoFilledEntity.TEST_FIELD)
            )
                    .queryList().any { e ->
                        Strings.areEqual(
                                e.getIdAsString(),
                                fieldNotFilled.getIdAsString()
                        ) || Strings.areEqual(
                                e.getIdAsString(),
                                fieldFilled.getIdAsString()
                        )
                    }
        }
        assertEquals(
                0, mango.select(MongoFilledEntity::class.java)
                .where(QueryBuilder.FILTERS.notExists(MongoFilledEntity.TEST_FIELD)).count()
        )
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo
    }
}
