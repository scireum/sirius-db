/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.db.mixing.FieldLookupCache
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class FieldLookupCacheSpec extends BaseSpecification {

    @Part
    private static OMA oma

    @Part
    private static FieldLookupCache lookupCache

    def "lookup works"() {
        given:
        SmartQueryTestEntity entity = new SmartQueryTestEntity()
        entity.setValue("Cache Test")
        entity.setTestNumber(12345)
        oma.update(entity)
        when:
        def value1 = lookupCache.lookup(SmartQueryTestEntity.class, entity.getId(), SmartQueryTestEntity.VALUE)
        def value2 = lookupCache.lookup(SmartQueryTestEntity.class, entity.getId(), SmartQueryTestEntity.TEST_NUMBER)
        def value3 = lookupCache.lookup(SmartQueryTestEntity.class, entity.getId(), SmartQueryTestEntity.VALUE)
        then:
        value1.asString() == "Cache Test"
        value2.asInt(0) == 12345
        value3.asString() == "Cache Test"
    }

}
