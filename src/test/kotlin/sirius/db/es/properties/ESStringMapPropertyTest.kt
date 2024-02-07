/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import sirius.db.es.Elastic
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class ESStringMapPropertySpec extends BaseSpecification {

    @Part
    private static Elastic elastic

            def "reading and writing works"() {
        when:
        def test = new ESStringMapEntity()
        test.getMap().put("Test", "1").put("Foo", "2")
        elastic.update(test)
        def resolved = elastic.refreshOrFail(test)
        then:
        resolved.getMap().size() == 2
        and:
        resolved.getMap().get("Test").get() == "1"
        resolved.getMap().get("Foo").get() == "2"

        when:
        resolved.getMap().modify().remove("Test")
        and:
        elastic.update(resolved)
        and:
        resolved = elastic.refreshOrFail(test)
        then:
        resolved.getMap().size() == 1
        and:
        !resolved.getMap().contains("Test")
        resolved.getMap().get("Foo").get() == "2"
    }

}
