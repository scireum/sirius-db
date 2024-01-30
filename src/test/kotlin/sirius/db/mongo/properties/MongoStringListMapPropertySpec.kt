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

class MongoStringListMapPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

            @Part
            private static Mongo mongo

            def "reading and writing works"() {
        when:
        def test = new MongoStringListMapEntity()
        test.getMap().add("Test", "1").add("Foo", "2").add("Test", "3")
        mango.update(test)
        def resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 2
        and:
        resolved.getMap().contains("Test", "1")
        resolved.getMap().contains("Test", "3")
        resolved.getMap().contains("Foo", "2")

        when:
        resolved.getMap().remove("Test", "1")
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 2
        and:
        !resolved.getMap().contains("Test", "1")
        resolved.getMap().contains("Test", "3")
        resolved.getMap().contains("Foo", "2")
    }

}
