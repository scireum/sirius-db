/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import sirius.db.mongo.properties.MongoStringListEntity
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class MongoFilterFactorySpec extends BaseSpecification {

    @Part
    private static Mango mango

    @Part
    private static Mongo mongo

    def "prefix search works"() {
        when:
        PrefixTestEntity test = new PrefixTestEntity()
        test.setPrefix("test-1")
        mango.update(test)
        then:
        mongo.find().
                where(QueryBuilder.FILTERS.prefix(PrefixTestEntity.PREFIX, "te")).
                singleIn(PrefixTestEntity.class).
                isPresent()
        and:
        mongo.find().
                where(QueryBuilder.FILTERS.prefix(PrefixTestEntity.PREFIX, "test-")).
                singleIn(PrefixTestEntity.class).
                isPresent()
        and:
        mongo.find().
                where(QueryBuilder.FILTERS.prefix(PrefixTestEntity.PREFIX, "Test-1")).
                singleIn(PrefixTestEntity.class).
                isPresent()
        and:
        mongo.find().
                where(QueryBuilder.FILTERS.text("Test-1")).
                singleIn(PrefixTestEntity.class).
                isPresent()
        and:
        mongo.find().
                where(QueryBuilder.FILTERS.text("Test")).
                singleIn(PrefixTestEntity.class).
                isPresent()
        and:
        mongo.find().
                where(QueryBuilder.FILTERS.text("test-1")).
                singleIn(PrefixTestEntity.class).
                isPresent()
        and:
        !mongo.find().
                where(QueryBuilder.FILTERS.text("te")).
                singleIn(PrefixTestEntity.class).
                isPresent()
    }
    def "oneInField query works"() {
        setup:
        MongoStringListEntity entity = new MongoStringListEntity()
        entity.getList().modify().addAll(["1", "2", "3"])
        MongoStringListEntity entityEmpty = new MongoStringListEntity()
        when:
        mango.update(entity)
        mango.update(entityEmpty)
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entity.getId())
             .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, ["2", "4", "5"]).build())
             .queryOne().getId() == entity.getId()
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entity.getId())
             .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, ["2", "3", "4"]).build())
             .queryOne().getId() == entity.getId()
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entity.getId())
             .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, ["4", "5", "6"]).build())
             .count() == 0
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entityEmpty.getId())
             .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, ["4", "5", "6"]).orEmpty().build())
             .queryOne().getId() == entityEmpty.getId()
    }

    def "noneInField query works"() {
        setup:
        MongoStringListEntity entity = new MongoStringListEntity()
        entity.getList().modify().addAll(["1", "2", "3"])
        when:
        mango.update(entity)
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entity.getId())
             .where(QueryBuilder.FILTERS.noneInField(MongoStringListEntity.LIST, ["2"]))
             .count() == 0
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entity.getId())
             .where(QueryBuilder.FILTERS.noneInField(MongoStringListEntity.LIST, ["5"]))
             .queryOne().getId() == entity.getId()
    }

    def "allInField query works"() {
        setup:
        MongoStringListEntity entity = new MongoStringListEntity()
        entity.getList().modify().addAll(["1", "2", "3"])
        when:
        mango.update(entity)
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entity.getId())
             .where(QueryBuilder.FILTERS.allInField(MongoStringListEntity.LIST, ["1", "2", "3", "4"]))
             .count() == 0
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entity.getId())
             .where(QueryBuilder.FILTERS.allInField(MongoStringListEntity.LIST, ["1", "2", "3"]))
             .queryOne().getId() == entity.getId()
        then:
        mango.select(MongoStringListEntity.class)
             .eq(MongoEntity.ID, entity.getId())
             .where(QueryBuilder.FILTERS.allInField(MongoStringListEntity.LIST, ["1", "2"]))
             .queryOne().getId() == entity.getId()
    }
}
