/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import sirius.db.mongo.Mango
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class MongoStringIntMapPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

    def "read/write a string int map works"() {
        when:
        def test = new MongoStringIntMapEntity()
        test.getMap().put("Test", 1).put("Foo", 2).put("Test", 3)
        mango.update(test)
        def resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 2
        and:
        resolved.getMap().get("Test").get() == 3
        resolved.getMap().get("Foo").get() == 2
        when:
        resolved.getMap().modify().remove("Test")
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 1
        and:
        !resolved.getMap().containsKey("Test")
        resolved.getMap().get("Foo").get() == 2
    }
}
