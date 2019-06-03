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

    private Optional<PrefixTestEntity> prefixSearch(String query) {
        return mongo.find().where(QueryBuilder.FILTERS.prefix(PrefixTestEntity.PREFIX, query))
                    .singleIn(PrefixTestEntity.class)
    }

    private Optional<PrefixTestEntity> textSearch(String query) {
        return mongo.find().where(QueryBuilder.FILTERS.text(query))
                    .singleIn(PrefixTestEntity.class)
    }

    def "prefix search works"() {
        when:
        PrefixTestEntity test = new PrefixTestEntity()
        test.setPrefix("test-1")
        mango.update(test)
        then:
        prefixSearch("te").isPresent()
        and:
        prefixSearch("test-").isPresent()
        and:
        prefixSearch("Test-1").isPresent()
        and:
        textSearch("Test-1").isPresent()
        and:
        textSearch("Test").isPresent()
        and:
        textSearch("test-1").isPresent()
        and:
        !textSearch("te").isPresent()
    }

    def "prefix with leading number works"() {
        when:
        PrefixTestEntity test = new PrefixTestEntity()
        test.setPrefix("1-test")
        mango.update(test)
        then:
        prefixSearch("1").isPresent()
        and:
        prefixSearch("1-t").isPresent()
        and:
        prefixSearch("1-test").isPresent()
        and:
        prefixSearch("1-TEST").isPresent()
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
