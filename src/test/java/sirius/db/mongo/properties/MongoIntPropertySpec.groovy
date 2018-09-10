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

class MongoIntPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

    def "read/write of int fields works"(){
        setup:
        MongoIntEntity obj = new MongoIntEntity()
        obj.setTestIntObject(new Integer(10))
        obj.setTestIntPrimitive(100)
        mango.update(obj)
        when:
        obj = mango.refreshOrFail(obj)
        then:
        obj.getTestIntObject() == 10
        obj.getTestIntPrimitive() == 100
    }
}
