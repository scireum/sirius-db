/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import sirius.db.mixing.Mapping
import sirius.db.mongo.properties.MongoStringListEntity
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Value
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

    def "containsAny query works"() {
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
                .where(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST, Value.of("2,4,5")).build())
                .queryOne().getId() == entity.getId()
                then:
                mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entity.getId())
                .where(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST, Value.of("2,3,4")).build())
                .queryOne().getId() == entity.getId()
                then:
                mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entity.getId())
                .where(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST, Value.of("4,5,6")).build())
                .count() == 0
                then:
                mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST, Value.of("4,5,6")).orEmpty().build())
                .queryOne().getId() == entityEmpty.getId()
    }

    def "complex OR-constraint can be inverted"() {
        setup:
        MongoStringListEntity entity = new MongoStringListEntity()
        entity.getList().modify().addAll(["1", "2", "3", "4"])
        MongoStringListEntity entityEmpty = new MongoStringListEntity()
        Mapping fakeField = Mapping.named("fakeField")
        when:
        mango.update(entity)
        mango.update(entityEmpty)
        then:
        mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entity.getId())
                .where(QueryBuilder.FILTERS.not(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST,
                        Value.of("4,5,6")).build()))
                .count() == 0
                then:
                mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(QueryBuilder.FILTERS.not(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST,
                        Value.of("4,5,6")).build()))
                .queryOne().getId() == entityEmpty.getId()
                then:
                mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(QueryBuilder.FILTERS.not(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST,
                        Value.of("4,5,6")).orEmpty().build()))
                .count() == 0
                then:
                mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(QueryBuilder.FILTERS.not(QueryBuilder.FILTERS.or(QueryBuilder.FILTERS.eq(fakeField, "someValue"),
                        QueryBuilder.FILTERS.eq(fakeField, "otherValue"))))
                .queryOne().getId() == entityEmpty.getId()
                then:
                mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(QueryBuilder.FILTERS.not(QueryBuilder.FILTERS.or(QueryBuilder.FILTERS.eq(fakeField, "someValue"),
                        QueryBuilder.FILTERS.eq(fakeField, "otherValue"),
                        QueryBuilder.FILTERS.notExists(fakeField))))
                .count() == 0
    }

    def "complex AND-constraint cannot be inverted"() {
        setup:
        MongoStringListEntity entity = new MongoStringListEntity()
        entity.getList().modify().addAll(["1", "2", "3"])
        MongoStringListEntity entityEmpty = new MongoStringListEntity()
        mango.update(entity)
        mango.update(entityEmpty)
        when:
        mango.select(MongoStringListEntity.class)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(QueryBuilder.FILTERS.not(QueryBuilder.FILTERS.containsAll(MongoStringListEntity.LIST,
                        Value.of("1,2,3,4")).build()))
                .queryOne()
                then:
                thrown IllegalArgumentException
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

    def "automatic 'and' works for fields"() {
        setup:
        mango.select(MangoTestEntity.class).delete()
                when:
        MangoTestEntity e1 = new MangoTestEntity()
                e1.setFirstname("AND")
                e1.setLastname("WORKS")
                mango.update(e1)
                then:
                mongo.find()
                .where(MangoTestEntity.LASTNAME, "WORKS")
                .countIn(MangoTestEntity.class) == 1
                        then:
                        mongo.find()
                        .where(MangoTestEntity.LASTNAME, "WORKS")
                        .where(MangoTestEntity.LASTNAME, "FAILS")
                        .where(MangoTestEntity.LASTNAME, "FAILS-YET-AGAIN")
                        .where(MangoTestEntity.LASTNAME, "FAILS-THE-LAST-TIME")
                        .countIn(MangoTestEntity.class) == 0
                                then:
                                mongo.find().where(MangoTestEntity.LASTNAME, "WORKS")
                                .where(MangoTestEntity.FIRSTNAME, "AND")
                                .countIn(MangoTestEntity.class) == 1
                                        then:
                                        mongo.find()
                                        .where(QueryBuilder.FILTERS.and(QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS"),
                                                QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND")))
                                        .where(QueryBuilder.FILTERS.and(QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "FAILS"),
                                                QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND")))
                                        .countIn(MangoTestEntity.class) == 0
                                                then:
                                                mongo.find()
                                                .where(QueryBuilder.FILTERS.and(QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS"),
                                                        QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND")))
                                                .where(QueryBuilder.FILTERS.and(QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS"),
                                                        QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND")))
                                                .countIn(MangoTestEntity.class) == 1
    }

    def "automatic 'and' works for multiple ands"() {
        when:
        MangoTestEntity e1 = new MangoTestEntity()
        e1.setFirstname("AND1")
        e1.setLastname("WORKS1")
        mango.update(e1)
        then:
        mongo.find()
                .where(QueryBuilder.FILTERS.and(QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS1"),
                        QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND1")))
                .where(QueryBuilder.FILTERS.and(QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "FAILS"),
                        QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND1")))
                .countIn(MangoTestEntity.class) == 0
                        then:
                        mongo.find()
                        .where(QueryBuilder.FILTERS.and(QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS1"),
                                QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND1")))
                        .where(QueryBuilder.FILTERS.and(QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS1"),
                                QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND1")))
                        .countIn(MangoTestEntity.class) == 1
    }

    def "isEmptyList works on List fields"() {
        setup:
        mango.select(MangoTestEntity.class).delete()
                when:
        MangoTestEntity e1 = new MangoTestEntity()
                e1.setFirstname("Peter")
                e1.setLastname("Parker")
                mango.update(e1)
                MangoTestEntity e2 = new MangoTestEntity()
                e2.setFirstname("Spider")
                e2.setLastname("Man")
                e2.getSuperPowers().add("Wallcrawling")
                mango.update(e2)
                then:
                mango.select(MangoTestEntity.class)
                .where(QueryBuilder.FILTERS.isEmptyList(MangoTestEntity.SUPER_POWERS)).count() == 1
    }

    def "forceEmpty works on List fields"() {
        setup:
        mango.select(MangoTestEntity.class).delete()
                when:
        MangoTestEntity e1 = new MangoTestEntity()
                e1.setFirstname("Peter")
                e1.setLastname("Parker")
                mango.update(e1)
                MangoTestEntity e2 = new MangoTestEntity()
                e2.setFirstname("Spider")
                e2.setLastname("Man")
                e2.getSuperPowers().add("Wallcrawling")
                mango.update(e2)
                then:
                mango.select(MangoTestEntity.class)
                .where(QueryBuilder.FILTERS.oneInField(MangoTestEntity.SUPER_POWERS, Collections.emptyList())
                        .forceEmpty().build()).count() == 1
    }

    def "hasListSize works on List fields"() {
        setup:
        mango.select(MangoTestEntity.class).delete()
                when:
        MangoTestEntity e1 = new MangoTestEntity()
                e1.setFirstname("Peter")
                e1.setLastname("Parker")
                e1.getSuperPowers().add("Häkeln");
        e1.getSuperPowers().add("Stricken");
        e1.getSuperPowers().add("Klöppeln");
        mango.update(e1)
        MangoTestEntity e2 = new MangoTestEntity()
        e2.setFirstname("Spider")
        e2.setLastname("Man")
        e2.getSuperPowers().add("Wallcrawling")
        e2.getSuperPowers().add("Unkrautjäten");
        e2.getSuperPowers().add("Rasenmähen");
        e2.getSuperPowers().add("Fensterputzen");
        mango.update(e2)
        then:
        mango.select(MangoTestEntity.class)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 0)).count() == 0
                mango.select(MangoTestEntity.class)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 1)).count() == 0
                mango.select(MangoTestEntity.class)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 2)).count() == 0
                mango.select(MangoTestEntity.class)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 3)).count() == 1
                mango.select(MangoTestEntity.class)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 4)).count() == 1
                mango.select(MangoTestEntity.class)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 5)).count() == 0
    }
}
