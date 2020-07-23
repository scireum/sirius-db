/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.db.jdbc.OMA
import sirius.db.jdbc.SmartQueryTestEntity
import sirius.db.mongo.Mango
import sirius.db.mongo.MangoTestEntity
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

import java.time.LocalDateTime

class FieldLookupCacheSpec extends BaseSpecification {

    @Part
    private static OMA oma

    @Part
    private static Mango mango

    @Part
    private static FieldLookupCache lookupCache

    def "jdbc field lookup works"() {
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
        value1 == "Cache Test"
        value2 == 12345
        value3 == "Cache Test"
    }

    def "mongo field lookup works"() {
        given:
        def localDateTime = LocalDateTime.now().minusYears(50)
        MangoTestEntity mangoTest = new MangoTestEntity()
        mangoTest.setFirstname("Gordon")
        mangoTest.setLastname("Ramsay")
        mangoTest.setAge(50)
        mangoTest.setBirthday(localDateTime)
        mangoTest.setCool(true)
        mangoTest.getSuperPowers().modify().addAll(Arrays.asList("Cooking", "Raging"))
        mango.update(mangoTest)
        when:
        def name1 = lookupCache.lookup(MangoTestEntity.class, mangoTest.getId(), MangoTestEntity.FIRSTNAME)
        def name2 = lookupCache.lookup(MangoTestEntity.class, mangoTest.getId(), MangoTestEntity.FIRSTNAME)
        def age = lookupCache.lookup(MangoTestEntity.class, mangoTest.getId(), MangoTestEntity.AGE)
        def birthday = lookupCache.lookup(MangoTestEntity.class, mangoTest.getId(), MangoTestEntity.BIRTHDAY)
        def cool = lookupCache.lookup(MangoTestEntity.class, mangoTest.getId(), MangoTestEntity.COOL)
        def powers = lookupCache.lookup(MangoTestEntity.class, mangoTest.getId(), MangoTestEntity.SUPER_POWERS)
        then:
        name1 == "Gordon"
        name2 == "Gordon"
        age == 50
        birthday == localDateTime
        cool == true
        powers == Arrays.asList("Cooking", "Raging")
    }

}
