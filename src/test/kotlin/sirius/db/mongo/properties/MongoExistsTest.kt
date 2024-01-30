/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import sirius.db.KeyGenerator
import sirius.db.mongo.*
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class MongoExistsSpec extends BaseSpecification {

    @Part
    private static Mango mango

            @Part
            private static Mongo mongo

            @Part
            private static KeyGenerator keyGen

            def "exists query works"() {
        when:
        Doc fieldMissing = mongo.insert().set(MongoEntity.ID, keyGen.generateId()).into(MongoExistsEntity.class)
                Doc fieldPresent = mongo.insert().
        set(MongoEntity.ID, keyGen.generateId()).
        set(MongoExistsEntity.TEST_FIELD, "test").
        into(MongoExistsEntity.class)
                then:
                mango.select(MongoExistsEntity.class)
                .eq(MongoExistsEntity.TEST_FIELD, null)
                .queryFirst().getIdAsString() == fieldMissing.getString(MongoEntity.ID)
                mango.select(MongoExistsEntity.class)
                .eq(MongoExistsEntity.TEST_FIELD, null)
                .count() == 1

                mango.select(MongoExistsEntity.class)
                .ne(MongoExistsEntity.TEST_FIELD, null)
                .queryFirst().getIdAsString() == fieldPresent.getString(MongoEntity.ID)
                mango.select(MongoExistsEntity.class)
                .ne(MongoExistsEntity.TEST_FIELD, null)
                .count() == 1

                mango.select(MongoExistsEntity.class)
                .where(QueryBuilder.FILTERS.exists(MongoExistsEntity.TEST_FIELD))
                .queryFirst().getIdAsString() == fieldPresent.getString(MongoEntity.ID)
                mango.select(MongoExistsEntity.class)
                .where(QueryBuilder.FILTERS.exists(MongoExistsEntity.TEST_FIELD))
                .count() == 1

                mango.select(MongoExistsEntity.class)
                .where(QueryBuilder.FILTERS.notExists(MongoExistsEntity.TEST_FIELD))
                .queryFirst().getIdAsString() == fieldMissing.getString(MongoEntity.ID)
                mango.select(MongoExistsEntity.class)
                .where(QueryBuilder.FILTERS.notExists(MongoExistsEntity.TEST_FIELD))
                .count() == 1

                mango.select(MongoExistsEntity.class)
                .where(QueryBuilder.FILTERS.filled(MongoExistsEntity.TEST_FIELD))
                .queryFirst().getIdAsString() == fieldPresent.getString(MongoEntity.ID)
                mango.select(MongoExistsEntity.class)
                .where(QueryBuilder.FILTERS.filled(MongoExistsEntity.TEST_FIELD))
                .count() == 1

                mango.select(MongoExistsEntity.class)
                .where(QueryBuilder.FILTERS.notFilled(MongoExistsEntity.TEST_FIELD))
                .queryFirst().getIdAsString() == fieldMissing.getString(MongoEntity.ID)
                mango.select(MongoExistsEntity.class)
                .where(QueryBuilder.FILTERS.notFilled(MongoExistsEntity.TEST_FIELD))
                .count() == 1
    }
}
