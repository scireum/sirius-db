/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import sirius.db.mongo.facets.MongoTermFacet
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class FacetSpec extends BaseSpecification {

    @Part
    private static Mango mango

    def "facet search works"() {
        given:
        MangoTestEntity te1 = new MangoTestEntity()
        te1.setFirstname("Hello")
        te1.setLastname("World")
        te1.setAge(999)
        mango.update(te1)
        and:
        MangoTestEntity te2 = new MangoTestEntity()
        te2.setFirstname("Hello")
        te2.setLastname("Moto")
        te2.setAge(999)
        mango.update(te2)
        and:
        MangoTestEntity te3 = new MangoTestEntity()
        te3.setFirstname("Loco")
        te3.setLastname("Moto")
        te3.setAge(999)
        mango.update(te3)
        and:
        MongoTermFacet firstnameFacet = new MongoTermFacet("firstname", MangoTestEntity.FIRSTNAME)
        MongoTermFacet lastnameFacet = new MongoTermFacet("lastname", MangoTestEntity.LASTNAME)
        when:
        mango.select(MangoTestEntity.class)
             .eq(MangoTestEntity.AGE, 999)
             .addFacet(firstnameFacet)
             .addFacet(lastnameFacet)
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
    }


}
