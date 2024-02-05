/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.db.jdbc.schema.Schema
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

import java.time.Duration

class StringPropertySpec extends BaseSpecification {

    @Part
    private static OMA oma

            @Part
            private static Schema schema

            def "reading and writing clobs works"() {
        given:
        schema.getReadyFuture().await(Duration.ofSeconds(45))
        and:
        TestClobEntity test = new TestClobEntity()
        when:
        test.setLargeValue("This is a test")
        and:
        oma.update(test)
        and:
        test = oma.refreshOrFail(test)
        then:
        test.getLargeValue() == "This is a test"
    }

    def "modification annotations are applied correctly"() {
        given:
        schema.getReadyFuture().await(Duration.ofSeconds(45))
        and:
        StringManipulationTestEntity test = new StringManipulationTestEntity()
        when:
        test.setTrimmed(" Test ")
        test.setLower(" TEST ")
        test.setUpper(" test ")
        test.setTrimmedLower(" TEST ")
        test.setTrimmedUpper(" test ")
        and:
        oma.update(test)
        and:
        test = oma.refreshOrFail(test)
        then:
        test.getTrimmed() == "Test"
        test.getLower() == " test "
        test.getUpper() == " TEST "
        test.getTrimmedLower() == "test"
        test.getTrimmedUpper() == "TEST"
    }

    def "modification annotations handle null correctly"() {
        given:
        schema.getReadyFuture().await(Duration.ofSeconds(45))
        and:
        StringManipulationTestEntity test = new StringManipulationTestEntity()
        when:
        oma.update(test)
        and:
        test = oma.refreshOrFail(test)
        then:
        test.getTrimmed() == null
        test.getLower() == null
        test.getTrimmedLower() == null
    }

}
