/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import sirius.db.es.Elastic
import sirius.db.mixing.properties.StringMapProperty
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
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
        and:
        Wait.seconds(2)
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
        Wait.seconds(2)
        resolved = elastic.refreshOrFail(resolved)
        then:
        resolved.getMap().size() == 1
        and:
        resolved.getMap().containsKey("b") && !resolved.getMap().get("b").isPresent()
    }

    def "querying date fields works"() {
        when:
        def test = new ESStringLocalDateTimeMapEntity()
        test.getMap().put("a", LocalDateTime.now().plusDays(2)).put("b", LocalDateTime.now().plusDays(3))
        elastic.update(test)
        Wait.seconds(2)
        then:
        elastic.select(ESStringLocalDateTimeMapEntity.class).
                where(Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).where(Elastic.FILTERS.gt(
                        ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.VALUE),
                        LocalDateTime.now().plusDays(1))).build()).count() == 1

        elastic.select(ESStringLocalDateTimeMapEntity.class).
                where(Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).where(Elastic.FILTERS.gt(
                        ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.VALUE),
                        LocalDateTime.now().plusDays(2))).build()).count() == 1

        elastic.select(ESStringLocalDateTimeMapEntity.class).
                where(Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).
                              where(Elastic.FILTERS.and(
                                      Elastic.FILTERS.eq(
                                              ESStringLocalDateTimeMapEntity.MAP.nested(
                                                      StringMapProperty.KEY), "a"),
                                      Elastic.FILTERS.gt(
                                              ESStringLocalDateTimeMapEntity.MAP.nested(
                                                      StringMapProperty.VALUE),
                                              LocalDateTime.now().plusDays(2)))).build()).count() == 0

        elastic.select(ESStringLocalDateTimeMapEntity.class).
                where(Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).
                              where(Elastic.FILTERS.and(
                                      Elastic.FILTERS.eq(
                                              ESStringLocalDateTimeMapEntity.MAP.nested(
                                                      StringMapProperty.KEY), "b"),
                                      Elastic.FILTERS.gt(
                                              ESStringLocalDateTimeMapEntity.MAP.nested(
                                                      StringMapProperty.VALUE),
                                              LocalDateTime.now().plusDays(2)))).build()).count() == 1

        elastic.select(ESStringLocalDateTimeMapEntity.class).
                where(Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).where(Elastic.FILTERS.gt(
                        ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.VALUE),
                        LocalDateTime.now().plusDays(3))).build()).count() == 0
    }
}
