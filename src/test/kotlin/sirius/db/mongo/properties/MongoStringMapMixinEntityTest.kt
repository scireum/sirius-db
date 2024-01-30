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


class MongoStringMapMixinEntitySpec extends BaseSpecification {

    @Part
    private static Mango mango

            def "reading and writing works"() {
        when:
        def test = new MongoStringMapMixinEntity()
        test.as(MongoStringMapMixin.class).getMapInMixin().put("key1", "value1").put("key2", "value2")
                mango.update(test)
                def refreshed = mango.refreshOrFail(test)
                then:
                def mixinOfRefreshed = refreshed.as(MongoStringMapMixin.class)
                mixinOfRefreshed.getMapInMixin().size() == 2
                mixinOfRefreshed.getMapInMixin().get("key1").get() == "value1"
                mixinOfRefreshed.getMapInMixin().get("key2").get() == "value2"

                when:
        def queried = mango.find(MongoStringMapMixinEntity.class, test.getId()).get()
                then:
                def mixinOfQueried = queried.as(MongoStringMapMixin.class)
                mixinOfQueried.getMapInMixin().size() == 2
                mixinOfQueried.getMapInMixin().get("key1").get() == "value1"
                mixinOfQueried.getMapInMixin().get("key2").get() == "value2"
    }
}
