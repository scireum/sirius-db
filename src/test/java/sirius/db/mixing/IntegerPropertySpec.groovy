/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part

import java.time.Duration

class IntegerPropertySpec extends BaseSpecification{
    @Part
    private static OMA oma

    def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "reading and writing long works"() {
        given:
        IntegerEntity test = new IntegerEntity()
        when:
        test.setIntValue(Integer.MAX_VALUE)
        and:
        oma.update(test)
        and:
        test = oma.refreshOrFail(test)
        then:
        test.getIntValue() == Integer.MAX_VALUE
    }

    def "default values work"() {
        given:
        IntegerEntity test = new IntegerEntity()
        Property intValue = test.getDescriptor().getProperty("intValue")
        when:
        intValue.parseValue(test, Value.of(null))
        and:
        oma.update(test)
        and:
        test = oma.refreshOrFail(test)
        then:
        test.getIntValue() == 100
    }
}
