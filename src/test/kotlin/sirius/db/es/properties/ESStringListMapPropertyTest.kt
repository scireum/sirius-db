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

class ESStringListMapPropertySpec extends BaseSpecification {

    @Part
    private static Elastic elastic

            def "reading and writing works"() {
        when:
        def test = new ESStringListMapEntity()
        test.getMap().add("Test", "1").add("Foo", "2").add("Test", "3")
        elastic.update(test)
        def resolved = elastic.refreshOrFail(test)
        then:
        resolved.getMap().size() == 2
        and:
        resolved.getMap().contains("Test", "1")
        resolved.getMap().contains("Test", "3")
        resolved.getMap().contains("Foo", "2")

        when:
        resolved.getMap().remove("Test", "1")
        and:
        elastic.update(resolved)
        and:
        resolved = elastic.refreshOrFail(test)
        then:
        resolved.getMap().size() == 2
        and:
        !resolved.getMap().contains("Test", "1")
        resolved.getMap().contains("Test", "3")
        resolved.getMap().contains("Foo", "2")
    }

}
