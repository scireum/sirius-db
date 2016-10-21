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

import java.time.Duration

class LongPropertySpec extends BaseSpecification {

    @Part
    private static OMA oma;

    def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60));
    }

    def "reading and writing long works"() {
        given:
        LongEntity test = new LongEntity();
        when:
        test.setLongValue(Long.MAX_VALUE);
        and:
        oma.update(test);
        and:
        test = oma.refreshOrFail(test);
        then:
        test.getLongValue() == Long.MAX_VALUE
    }

}
