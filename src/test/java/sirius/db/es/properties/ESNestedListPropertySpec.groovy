/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import sirius.db.es.Elastic
import sirius.db.es.filter.NestedQuery
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part

class ESNestedListPropertySpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def "reading, change tracking and writing works"() {
        when:
        def test = new ESNestedListEntity()
        test.getList().add(new ESNestedListEntity.NestedEntity().withValue1("X").withValue2("Y"))
        elastic.update(test)
        Wait.seconds(2)
        def resolved = elastic.refreshOrFail(test)
        then:
        resolved.getList().size() == 1
        and:
        resolved.getList().data().get(0).getValue1() == "X"
        resolved.getList().data().get(0).getValue2() == "Y"

        when:
        resolved.getList().modify().get(0).withValue1("Z")
        and:
        elastic.update(resolved)
        Wait.seconds(2)
        and:
        resolved = elastic.refreshOrFail(test)
        then:
        resolved.getList().size() == 1
        and:
        resolved.getList().data().get(0).getValue1() == "Z"
        resolved.getList().data().get(0).getValue2() == "Y"

        when:
        resolved.getList().modify().remove(0)
        and:
        elastic.update(resolved)
        Wait.seconds(2)
        and:
        resolved = elastic.refreshOrFail(test)
        then:
        resolved.getList().size() == 0
    }

    /**
     * When storing lists of nested objects, ES will ensure that when executing a "nested" query,
     * an entity only matches, if all fields in a single nested objects matches.
     * <p>
     * Otherwise an entity would match if one property in any nested object matches.
     */
    def "searching in nested fields works as expected"() {
        when:
        def test = new ESNestedListEntity()
        test.getList().add(new ESNestedListEntity.NestedEntity().withValue1("A").withValue2("B"))
        test.getList().add(new ESNestedListEntity.NestedEntity().withValue1("C").withValue2("D"))
        elastic.update(test)
        test = new ESNestedListEntity()
        test.getList().add(new ESNestedListEntity.NestedEntity().withValue1("A").withValue2("B"))
        test.getList().add(new ESNestedListEntity.NestedEntity().withValue1("A").withValue2("D"))
        elastic.update(test)
        Wait.seconds(2)
        def query = elastic.select(ESNestedListEntity.class).
                filter(
                        new NestedQuery(ESNestedListEntity.LIST).eq(
                                ESNestedListEntity.LIST.nested(ESNestedListEntity.NestedEntity.VALUE1),
                                "A").
                                eq(ESNestedListEntity.LIST.nested(ESNestedListEntity.NestedEntity.VALUE2), "D"));
        def resolved = query.queryFirst()
        then:
        query.limit(0).count() == 1
        resolved.getList().size() == 2
        and:
        resolved.getList().data().get(0).getValue1() == "A"
        resolved.getList().data().get(0).getValue2() == "B"
        resolved.getList().data().get(1).getValue1() == "A"
        resolved.getList().data().get(1).getValue2() == "D"
    }

}
