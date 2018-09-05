/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import sirius.db.mongo.*
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Strings
import sirius.kernel.di.std.Part

import java.util.function.Predicate

class MongoFilledSpec extends BaseSpecification {

    @Part
    private static Mango mango

    @Part
    private static Mongo mongo

    def "filled/notFilled query works"() {
        setup:
        MongoFilledEntity fieldFilled = new MongoFilledEntity()
        fieldFilled.setTestField("test")
        MongoFilledEntity fieldNotFilled = new MongoFilledEntity()
        when:
        mango.update(fieldFilled)
        mango.update(fieldNotFilled)
        then:
        mango.select(MongoFilledEntity.class)
             .eq(MongoFilledEntity.TEST_FIELD, null)
             .queryFirst().getIdAsString() == fieldNotFilled.getIdAsString() && mango.select(MongoFilledEntity.class)
                                                                                     .eq(MongoFilledEntity.TEST_FIELD, null).count() == 1
        mango.select(MongoFilledEntity.class)
             .ne(MongoFilledEntity.TEST_FIELD, null)
             .queryFirst().getIdAsString() == fieldFilled.getIdAsString() && mango.select(MongoFilledEntity.class)
                                                                                  .ne(MongoFilledEntity.TEST_FIELD, null).count() == 1

        mango.select(MongoFilledEntity.class)
             .where(QueryBuilder.FILTERS.filled(MongoFilledEntity.TEST_FIELD))
             .queryFirst().getIdAsString() == fieldFilled.getIdAsString() && mango.select(MongoFilledEntity.class)
                                                                                  .where(QueryBuilder.FILTERS.filled(MongoFilledEntity.TEST_FIELD)).count() == 1

        mango.select(MongoFilledEntity.class)
             .where(QueryBuilder.FILTERS.notFilled(MongoFilledEntity.TEST_FIELD))
             .queryFirst().getIdAsString() == fieldNotFilled.getIdAsString() && mango.select(MongoFilledEntity.class)
                                                                                     .where(QueryBuilder.FILTERS.notFilled(MongoFilledEntity.TEST_FIELD)).count() == 1

        mango.select(MongoFilledEntity.class)
             .where(QueryBuilder.FILTERS.exists(MongoFilledEntity.TEST_FIELD))
             .queryList()
             .every({e -> Strings.areEqual(e.getIdAsString(), fieldNotFilled.getIdAsString()) || Strings.areEqual(e.getIdAsString(), fieldFilled.getIdAsString()) })

        mango.select(MongoFilledEntity.class)
             .where(QueryBuilder.FILTERS.notExists(MongoFilledEntity.TEST_FIELD)).count() == 0
    }
}
