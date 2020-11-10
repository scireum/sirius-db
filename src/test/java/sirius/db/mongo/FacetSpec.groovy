/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import sirius.db.mixing.DateRange
import sirius.db.mongo.facets.MongoBooleanFacet
import sirius.db.mongo.facets.MongoDateRangeFacet
import sirius.db.mongo.facets.MongoTermFacet
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

import java.time.LocalDateTime

class FacetSpec extends BaseSpecification {

    @Part
    private static Mango mango

    def "facet search works"() {
        given:
        MangoTestEntity te1 = new MangoTestEntity()
        te1.setFirstname("Hello")
        te1.setLastname("World")
        te1.setAge(999)
        te1.setBirthday(LocalDateTime.now())
        te1.setCool(true)
        te1.getSuperPowers().modify().addAll(Arrays.asList("Flying", "X-ray vision"))
        mango.update(te1)
        and:
        MangoTestEntity te2 = new MangoTestEntity()
        te2.setFirstname("Hello")
        te2.setLastname("Moto")
        te2.setBirthday(LocalDateTime.now().minusDays(1))
        te2.setAge(999)
        te2.setCool(true)
        te2.getSuperPowers().modify().addAll(Arrays.asList("Flying", "X-ray vision"))
        mango.update(te2)
        and:
        MangoTestEntity te3 = new MangoTestEntity()
        te3.setFirstname("Loco")
        te3.setLastname("Moto")
        te3.setAge(999)
        te3.getSuperPowers().modify().addAll(Arrays.asList("Flying", "Time travel"))
        mango.update(te3)
        and:
        MongoTermFacet firstnameFacet = new MongoTermFacet(MangoTestEntity.FIRSTNAME)
        MongoTermFacet lastnameFacet = new MongoTermFacet(MangoTestEntity.LASTNAME)
        MongoBooleanFacet coolFacet = new MongoBooleanFacet(MangoTestEntity.COOL)
        MongoTermFacet superPowersFacet = new MongoTermFacet(MangoTestEntity.SUPER_POWERS)
        MongoDateRangeFacet datesFacet = new MongoDateRangeFacet(MangoTestEntity.BIRTHDAY,
                                                                 Arrays.asList(DateRange.TODAY,
                                                                               DateRange.YESTERDAY,
                                                                               new DateRange("both",
                                                                                             { -> "both" },
                                                                                             { ->
                                                                                                 DateRange.
                                                                                                         YESTERDAY.
                                                                                                         getFrom()
                                                                                             },
                                                                                             { ->
                                                                                                 DateRange.TODAY.
                                                                                                         getUntil()
                                                                                             }),
                                                                               DateRange.BEFORE_LAST_YEAR))
        when:
        mango.select(MangoTestEntity.class)
             .eq(MangoTestEntity.AGE, 999)
             .addFacet(firstnameFacet)
             .addFacet(lastnameFacet)
             .addFacet(coolFacet)
             .addFacet(datesFacet)
             .addFacet(superPowersFacet)
             .executeFacets()
        then:
        lastnameFacet.getValues().get(0).getFirst() == "Moto"
        lastnameFacet.getValues().get(0).getSecond() == 2
        lastnameFacet.getValues().get(1).getFirst() == "World"
        lastnameFacet.getValues().get(1).getSecond() == 1
        and:
        firstnameFacet.getValues().get(0).getFirst() == "Hello"
        firstnameFacet.getValues().get(0).getSecond() == 2
        firstnameFacet.getValues().get(1).getFirst() == "Loco"
        firstnameFacet.getValues().get(1).getSecond() == 1
        and:
        coolFacet.getNumTrue() == 2
        coolFacet.getNumFalse() == 1
        and:
        datesFacet.getRanges().get(0).getSecond() == 1
        datesFacet.getRanges().get(1).getSecond() == 1
        datesFacet.getRanges().get(2).getSecond() == 2
        datesFacet.getRanges().get(3).getSecond() == 0
        and:
        superPowersFacet.getValues().get(0).getFirst() == "Flying"
        superPowersFacet.getValues().get(0).getSecond() == 3
        superPowersFacet.getValues().get(1).getFirst() == "X-ray vision"
        superPowersFacet.getValues().get(1).getSecond() == 2
        superPowersFacet.getValues().get(2).getFirst() == "Time travel"
        superPowersFacet.getValues().get(2).getSecond() == 1
    }
}
