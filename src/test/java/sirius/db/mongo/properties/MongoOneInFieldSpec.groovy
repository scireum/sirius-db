/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import sirius.db.mongo.Mango
import sirius.db.mongo.QueryBuilder
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class MongoOneInFieldSpec extends BaseSpecification {

    @Part
    private static Mango mango

    def "OneInField query works"() {
        setup:
        MongoStringListEntity entity = new MongoStringListEntity()
        entity.getList().modify().addAll(["1", "2", "3"])
        when:
        mango.update(entity)
        then:
        mango.select(MongoStringListEntity.class)
             .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, ["2", "4", "5"]).build())
             .queryOne().getId() == entity.getId()
        then:
        mango.select(MongoStringListEntity.class)
             .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, ["2", "3", "4"]).build())
             .queryOne().getId() == entity.getId()
        then:
        mango.select(MongoStringListEntity.class)
             .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, ["4", "5", "6"]).build())
             .count() == 0
    }
}
