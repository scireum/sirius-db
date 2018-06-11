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

class MongoStringMapPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

    @Part
    private static Mongo mongo

    def "reading and writing works"() {
        when:
        def test = new MongoStringMapEntity()
        test.getMap().put("Test", "1").put("Foo", "2")
        mango.update(test)
        def resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 2
        and:
        resolved.getMap().get("Test").get() == "1"
        resolved.getMap().get("Foo").get() == "2"

        when:
        resolved.getMap().modify().remove("Test")
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 1
        and:
        !resolved.getMap().contains("Test")
        resolved.getMap().get("Foo").get() == "2"
    }

}
