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
import sirius.db.KeyGenerator
import sirius.db.mongo.Mango
import sirius.db.mongo.Mongo
import sirius.db.mongo.MongoEntity
import sirius.db.mongo.QueryBuilder
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class MongoExistsSpec {
    @Test
    fun `exists query works`() {

        val fieldMissing = mongo.insert().set(MongoEntity.ID, keyGen.generateId()).into(MongoExistsEntity::class.java)
        val fieldPresent =
                mongo.insert().set(MongoEntity.ID, keyGen.generateId()).set(MongoExistsEntity.TEST_FIELD, "test")
                        .into(MongoExistsEntity::class.java)

        assertEquals(
                fieldMissing.getString(MongoEntity.ID), mango.select(MongoExistsEntity::class.java)
                .eq(MongoExistsEntity.TEST_FIELD, null)
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoExistsEntity::class.java)
                .eq(MongoExistsEntity.TEST_FIELD, null)
                .count()
        )

        assertEquals(
                fieldPresent.getString(MongoEntity.ID), mango.select(MongoExistsEntity::class.java)
                .ne(MongoExistsEntity.TEST_FIELD, null)
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoExistsEntity::class.java)
                .ne(MongoExistsEntity.TEST_FIELD, null)
                .count()
        )

        assertEquals(
                fieldPresent.getString(MongoEntity.ID), mango.select(MongoExistsEntity::class.java)
                .where(QueryBuilder.FILTERS.exists(MongoExistsEntity.TEST_FIELD))
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoExistsEntity::class.java)
                .where(QueryBuilder.FILTERS.exists(MongoExistsEntity.TEST_FIELD))
                .count()
        )

        assertEquals(
                fieldMissing.getString(MongoEntity.ID), mango.select(MongoExistsEntity::class.java)
                .where(QueryBuilder.FILTERS.notExists(MongoExistsEntity.TEST_FIELD))
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoExistsEntity::class.java)
                .where(QueryBuilder.FILTERS.notExists(MongoExistsEntity.TEST_FIELD))
                .count()
        )

        assertEquals(
                fieldPresent.getString(MongoEntity.ID), mango.select(MongoExistsEntity::class.java)
                .where(QueryBuilder.FILTERS.filled(MongoExistsEntity.TEST_FIELD))
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoExistsEntity::class.java)
                .where(QueryBuilder.FILTERS.filled(MongoExistsEntity.TEST_FIELD))
                .count()
        )

        assertEquals(
                fieldMissing.getString(MongoEntity.ID), mango.select(MongoExistsEntity::class.java)
                .where(QueryBuilder.FILTERS.notFilled(MongoExistsEntity.TEST_FIELD))
                .queryFirst().getIdAsString()
        )
        assertEquals(
                1, mango.select(MongoExistsEntity::class.java)
                .where(QueryBuilder.FILTERS.notFilled(MongoExistsEntity.TEST_FIELD))
                .count()
        )
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo

        @Part
        private lateinit var keyGen: KeyGenerator
    }
}
