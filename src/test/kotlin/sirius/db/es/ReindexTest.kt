/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part

class ReindexSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

            def "reindex and move alias works"() {
        given:
        ElasticTestEntity e = new ElasticTestEntity()
        e.setAge(10)
        e.setFirstname("test")
        e.setLastname("test")
        and:
        elastic.update(e)
        and:
        elastic.refresh(ElasticTestEntity.class)

                when:
        elastic.getLowLevelClient().startReindex(elastic.determineReadAlias(e.getDescriptor()), "reindex-test")
        and:
        Wait.seconds(2)
        then:
        elastic.getLowLevelClient().indexExists("reindex-test")

        when:
        elastic.getLowLevelClient().createOrMoveAlias(elastic.determineReadAlias(e.getDescriptor()), "reindex-test")
        and:
        def indicesForAlias = elastic.determineEffectiveIndex(e.getDescriptor())
        then:
        indicesForAlias == "reindex-test"
        and:
        elastic.find(ElasticTestEntity.class, e.getId()).isPresent()
    }
}
