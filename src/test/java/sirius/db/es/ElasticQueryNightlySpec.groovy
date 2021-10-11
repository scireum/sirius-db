/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import org.junit.jupiter.api.Tag
import sirius.kernel.BaseSpecification
import sirius.kernel.Tags
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

import java.time.Duration

@Tag(Tags.NIGHTLY)
class ElasticQueryNightlySpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "scroll query / large streamBlockwise work"() {
        when:
        for (int i = 1; i <= 1500; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("SCROLL")
            entity.setCounter(i)
            elastic.update(entity)
        }
        elastic.refresh(QueryTestEntity.class)
        and:
        int sum = 0
        elastic.select(QueryTestEntity.class).
                eq(QueryTestEntity.VALUE, "SCROLL").
                iterateAll({ e -> sum += e.getCounter() })
        then:
        sum == (1500 * 1501) / 2
        and:
        elastic.select(QueryTestEntity.class).
                eq(QueryTestEntity.VALUE, "SCROLL").
                streamBlockwise().
                mapToInt({ e -> e.getCounter() }).sum() == sum
    }

    def "selecting over 1000 entities in queryList throws an exception"() {
        given:
        elastic.select(ESListTestEntity.class).delete()
        and:
        for (int i = 0; i < 1001; i++) {
            def entityToCreate = new ESListTestEntity()
            entityToCreate.setCounter(i)
            elastic.update(entityToCreate)
        }
        elastic.refresh(ESListTestEntity.class)
        when:
        elastic.select(ESListTestEntity.class).queryList()
        then:
        thrown(HandledException)
    }

}
