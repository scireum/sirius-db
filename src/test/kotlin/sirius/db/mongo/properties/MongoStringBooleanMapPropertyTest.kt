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

class MongoStringBooleanMapPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

            def "read/write a string boolean map works"() {
        when:
        def test = new MongoStringBooleanMapEntity()
        test.getMap().put("a", true).put("b", false).put("c", new Boolean(true)).put("d", new Boolean(false))
        mango.update(test)
        def resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 4
        and:
        resolved.getMap().get("a").get()
        !resolved.getMap().get("b").get()
        resolved.getMap().get("c").get()
        !resolved.getMap().get("d").get()
        when:
        resolved.getMap().modify().remove("a")
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 3
        and:
        !resolved.getMap().containsKey("a")
        resolved.getMap().get("c").get()
    }
}
