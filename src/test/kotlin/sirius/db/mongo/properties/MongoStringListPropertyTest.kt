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
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class MongoStringListPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

            @Part
            private static Mongo mongo

            def "reading and writing works for MongoDB"() {
        when:
        def test = new MongoStringListEntity()
        test.getList().add("Test").add("Hello").add("World")
        mango.update(test)
        def resolved = mango.refreshOrFail(test)
        then:
        resolved.getList().size() == 3
        and:
        resolved.getList().contains("Test")
        resolved.getList().contains("Hello")
        resolved.getList().contains("World")

        when:
        mongo.update().pull(MongoStringListEntity.LIST, "Hello").executeFor(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getList().size() == 2
        and:
        resolved.getList().contains("Test")
        !resolved.getList().contains("Hello")
        resolved.getList().contains("World")

        when:
        resolved.getList().modify().remove("World")
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getList().size() == 1
        and:
        resolved.getList().contains("Test")
        !resolved.getList().contains("Hello")
        !resolved.getList().contains("World")

        when:
        mongo.update().addEachToSet(MongoStringListEntity.LIST, ["a", "b", "c", "Test"]).executeFor(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getList().size() == 4
        and:
        resolved.getList().contains("Test")
        resolved.getList().contains("a")
        resolved.getList().contains("b")
        resolved.getList().contains("c")
    }
}
