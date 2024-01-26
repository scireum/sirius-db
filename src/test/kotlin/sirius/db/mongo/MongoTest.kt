/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import sirius.db.KeyGenerator
import sirius.db.mixing.Mapping
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class MongoSpec extends BaseSpecification {

    @Part
    private static Mongo mongo

            @Part
            private static KeyGenerator keyGen

            def "basic read / write works"() {
        given:
        def testString = String.valueOf(System.currentTimeMillis())
        when:
        def result = mongo.insert().set("test", testString).set("id", keyGen.generateId()).into("test")
        then:
        mongo.find().
        where("id", result.getString("id")).
        singleIn("test").
        map({ d -> d.getString("test") }).
        orElse(null) == testString
    }

    def "read from secondary works"() {
        given:
        def testString = String.valueOf(System.currentTimeMillis())
        when:
        def result = mongo.insert().set("test", testString).set("id", keyGen.generateId()).into("test")
        then:
        mongo.findInSecondary().
        where("id", result.getString("id")).
        singleIn("test").
        map({ d -> d.getString("test") }).
        orElse(null) == testString
    }

    def "sort works for singleIn"() {
        when:
        def result1 = mongo.insert().set("sortBy", 1).set("id", keyGen.generateId()).into("test")
        def result2 = mongo.insert().set("sortBy", 3).set("id", keyGen.generateId()).into("test")
        def result3 = mongo.insert().set("sortBy", 2).set("id", keyGen.generateId()).into("test")
        then:
        mongo.find()
                .orderByDesc("sortBy")
                .singleIn("test")
                .map({ entity -> entity.getString("id") })
                .orElse(null) == result2.getString("id")
    }

    def "aggregation works"() {
        when:
        def result1 = mongo.insert().set("filter", 1).set("value", 9).set("id", keyGen.generateId()).into("test2")
        def result2 = mongo.insert().set("filter", 4).set("value", 29).set("id", keyGen.generateId()).into("test2")
        def result3 = mongo.insert().set("filter", 2).set("value", 22).set("id", keyGen.generateId()).into("test2")
        then:
        mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 5))
                .aggregateIn("test2", Mapping.named("value"), "\$sum").isNull()
        then:
        mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$sum").asInt(0) == 60
        and:
        mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$sum").asInt(0) == 51
        and:
        mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$avg").asDouble(0.0) == 20
        and:
        mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$avg").asDouble(0.0) == 25.5
        and:
        mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$min").asInt(0) == 9
        and:
        mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$min").asInt(0) == 22
        and:
        mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$max").asInt(0) == 29
        and:
        mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$max").asInt(0) == 29
        and:
        mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$push").get(List.class, []) == [9, 29, 22]
        and:
        mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$push").get(List.class, []) == [29, 22]
    }
}
