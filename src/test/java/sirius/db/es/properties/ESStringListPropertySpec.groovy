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

class ESStringListPropertySpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def "reading and writing works for Elasticsearch"() {
        when:
        def test = new ESStringListEntity()
        test.getList().add("Test").add("Hello").add("World")
        elastic.update(test)
        def resolved = elastic.refreshOrFail(test)
        then:
        resolved.getList().size() == 3
        and:
        resolved.getList().contains("Test")
        resolved.getList().contains("Hello")
        resolved.getList().contains("World")

        when:
        resolved.getList().modify().remove("World")
        and:
        elastic.update(resolved)
        and:
        resolved = elastic.refreshOrFail(test)
        then:
        resolved.getList().size() == 2
        and:
        resolved.getList().contains("Test")
        resolved.getList().contains("Hello")
        !resolved.getList().contains("World")
    }

}
