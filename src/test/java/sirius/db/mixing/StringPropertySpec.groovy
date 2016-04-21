/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class StringPropertySpec extends BaseSpecification {

    @Part
    private static OMA oma;

    def "reading and writing clobs works"() {
        given:
        TestClobEntity test = new TestClobEntity();
        when:
        test.setLargeValue("This is a test");
        and:
        oma.update(test);
        and:
        test = oma.refreshOrFail(test);
        then:
        test.getLargeValue() == "This is a test"
    }

}
