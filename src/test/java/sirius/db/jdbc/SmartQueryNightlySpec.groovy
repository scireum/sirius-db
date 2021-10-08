/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import org.junit.jupiter.api.Tag
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

@Tag("nightly")
class SmartQueryNightlySpec extends BaseSpecification {

    @Part
    static OMA oma

    def "selecting over 1000 entities in queryList throws an exception"() {
        given:
        oma.select(ListTestEntity.class).delete()
        and:
        for (int i = 0; i < 1001; i++) {
            def entityToCreate = new ListTestEntity()
            entityToCreate.setCounter(i)
            oma.update(entityToCreate)
        }
        when:
        oma.select(ListTestEntity.class).queryList()
        then:
        thrown(HandledException)
    }

}
