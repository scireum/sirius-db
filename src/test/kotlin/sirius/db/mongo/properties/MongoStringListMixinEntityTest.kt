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

class MongoStringListMixinEntitySpec extends BaseSpecification {

    @Part
    private static Mango mango

            def "reading and writing works"() {
        when:
        def test = new MongoStringListMixinEntity()
        test.as(MongoStringListMixin.class).getListInMixin().add("hello").add("world")
                mango.update(test)
                def refreshed = mango.refreshOrFail(test)
                then:
                def mixinOfRefreshed = refreshed.as(MongoStringListMixin.class)
                mixinOfRefreshed.getListInMixin().size() == 2
                mixinOfRefreshed.getListInMixin().data().get(0) == "hello"
                mixinOfRefreshed.getListInMixin().data().get(1) == "world"

                when:
        def queried = mango.find(MongoStringListMixinEntity.class, test.getId()).get()
                then:
                def mixinOfQueried = queried.as(MongoStringListMixin.class)
                mixinOfQueried.getListInMixin().size() == 2
                mixinOfQueried.getListInMixin().data().get(0) == "hello"
                mixinOfQueried.getListInMixin().data().get(1) == "world"
    }
}
