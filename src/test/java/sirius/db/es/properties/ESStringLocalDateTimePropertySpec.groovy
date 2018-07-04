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

import java.time.LocalDateTime

class ESStringLocalDateTimePropertySpec extends BaseSpecification {
    @Part
    private static Elastic elastic

    def "reading and writing works for Elasticsearch"() {
        when:
        def test = new ESStringLocalDateTimeMapEntity()
        def now = LocalDateTime.now()
        test.getMap().put("a", now)
        elastic.update(test)
        def resolved = elastic.refreshOrFail(test)
        then:
        resolved.getMap().size() == 1
        and:
        resolved.getMap().get("a").isPresent() && resolved.getMap().get("a").get() == now

        when:
        resolved.getMap().modify().remove("a")
        resolved.getMap().modify().put("b", null)
        and:
        elastic.update(resolved)
        and:
        resolved = elastic.refreshOrFail(resolved)
        then:
        resolved.getMap().size() == 1
        and:
        resolved.getMap().containsKey("b") && !resolved.getMap().get("b").isPresent()
    }
}
