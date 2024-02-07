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

class ESStringBooleanMapPropertySpec extends BaseSpecification {

    @Part
    private static Elastic elastic

            def "read/write a string boolean map works"() {
        when:
        def test = new ESStringBooleanMapEntity()
        test.getMap().put("a", true).put("b", false).put("c", new Boolean(true)).put("d", new Boolean(false))
        elastic.update(test)
        def resolved = elastic.refreshOrFail(test)
        then:
        resolved.getMap().size() == 4
        and:
        resolved.getMap().get("a").get()
        !resolved.getMap().get("b").get()
        resolved.getMap().get("c").get()
        !resolved.getMap().get("d").get()
        when:
        resolved.getMap().modify().remove("a")
        and:
        elastic.update(resolved)
        and:
        resolved = elastic.refreshOrFail(test)
        then:
        resolved.getMap().size() == 3
        and:
        !resolved.getMap().containsKey("a")
        resolved.getMap().get("c").get()
    }
}
