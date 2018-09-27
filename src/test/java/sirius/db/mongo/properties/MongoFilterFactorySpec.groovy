/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import sirius.db.mongo.Mango
import sirius.db.mongo.Mongo
import sirius.db.mongo.PrefixTestEntity
import sirius.db.mongo.QueryBuilder
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class MongoFilterFactorySpec extends BaseSpecification {

    @Part
    private static Mongo mongo

    @Part
    private static Mango mango

    def "prefix search works"() {
        when:
        PrefixTestEntity test = new PrefixTestEntity()
        test.setPrefix("test-1")
        mango.update(test)
        and:
        print mongo.find().
                      where(QueryBuilder.FILTERS.prefix(PrefixTestEntity.PREFIX, "te")).explain(PrefixTestEntity.class)

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

}
