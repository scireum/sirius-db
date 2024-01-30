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
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

class MongoIntPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

            def "read/write of int fields works"() {
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

    def "no errors if all fields are within the annotated ranges"() {
        setup:
        MongoIntEntity obj = new MongoIntEntity()
        obj.setTestIntObject(new Integer(10))
        obj.setTestIntPrimitive(100)
        obj.setTestIntPositive(14)
        obj.setTestIntPositiveWithZero(0)
        obj.setTestIntMaxHundred(90)
        obj.setTestIntMinHundred(120)
        obj.setTestIntTwentys(23)
        when:
        mango.update(obj)
        then:
        noExceptionThrown()
    }

    def "error for non positive field"() {
        setup:
        MongoIntEntity obj = new MongoIntEntity()
        obj.setTestIntObject(new Integer(10))
        obj.setTestIntPrimitive(100)
        obj.setTestIntPositive(-1)
        when:
        mango.update(obj)
        then:
        def e = thrown(HandledException.class)
                e.getMessage() == (obj.getDescriptor().
        getProperty(MongoIntEntity.TEST_INT_POSITIVE).
        illegalFieldValue(Value.of(-1))).getMessage()
    }

    def "error for too small value"() {
        setup:
        MongoIntEntity obj = new MongoIntEntity()
        obj.setTestIntObject(new Integer(10))
        obj.setTestIntPrimitive(100)
        obj.setTestIntMinHundred(99)
        when:
        mango.update(obj)
        then:
        def e = thrown(HandledException.class)
                e.getMessage() == (obj.getDescriptor().
        getProperty(MongoIntEntity.TEST_INT_MIN_HUNDRED).
        illegalFieldValue(Value.of(99))).getMessage()
    }

    def "error for too big value"() {
        setup:
        MongoIntEntity obj = new MongoIntEntity()
        obj.setTestIntObject(new Integer(10))
        obj.setTestIntPrimitive(100)
        obj.setTestIntMaxHundred(111)
        when:
        mango.update(obj)
        then:
        def e = thrown(HandledException.class)
                e.getMessage() == (obj.getDescriptor().
        getProperty(MongoIntEntity.TEST_INT_MAX_HUNDRED).
        illegalFieldValue(Value.of(111))).getMessage()
    }

}
