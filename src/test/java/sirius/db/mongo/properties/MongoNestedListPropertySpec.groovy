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

class MongoNestedListPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

    @Part
    private static Mongo mongo

    def "reading, change tracking and writing works"() {
        when:
        def test = new MongoNestedListEntity()
        test.getList().add(new MongoNestedListEntity.NestedEntity().withValue1("X").withValue2("Y"))
        mango.update(test)
        def resolved = mango.refreshOrFail(test)
        then:
        resolved.getList().size() == 1
        and:
        resolved.getList().data().get(0).getValue1() == "X"
        resolved.getList().data().get(0).getValue2() == "Y"

        when:
        resolved.getList().modify().get(0).withValue1("Z")
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getList().size() == 1
        and:
        resolved.getList().data().get(0).getValue1() == "Z"
        resolved.getList().data().get(0).getValue2() == "Y"

        when:
        resolved.getList().modify().remove(0)
        and:
        mango.update(resolved)
        and:
        resolved = mango.refreshOrFail(test)
        then:
        resolved.getList().size() == 0
    }

}
