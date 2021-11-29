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

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

class MongoStringNestedMapPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

    @Part
    private static Mongo mongo

    def "reading and writing works"() {
        when:
        def test = new MongoStringNestedMapEntity()
        def timestamp = LocalDateTime.now().minusDays(2)
        test.getMap().put("X", new MongoStringNestedMapEntity.NestedEntity().withValue1("Y").withValue2(timestamp))
        mango.update(test)
        def resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 1
        and:
        resolved.getMap().containsKey("X")
        resolved.getMap().get("X").get().getValue1() == "Y"
        resolved.getMap().get("X").get().getValue2() == timestamp.truncatedTo(ChronoUnit.MILLIS)

        when:
        resolved.getMap().modify().get("X").withValue1("ZZZ")
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 1
        and:
        resolved.getMap().containsKey("X")
        resolved.getMap().get("X").get().getValue1() == "ZZZ"

        when:
        resolved.getMap().modify().remove("X")
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getMap().size() == 0
    }

}
