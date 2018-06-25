/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import sirius.db.es.filter.Prefix
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Strings
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part

import java.time.Duration

class ElasticQuerySpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "queryList works"() {
        when:
        for (int i = 0; i < 100; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("SELECT")
            entity.setCounter(i)
            elastic.update(entity)
        }
        Wait.seconds(2)
        def entities = elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "SELECT").queryList()
        then:
        entities.size() == 100
        and:
        Strings.isFilled(entities.get(0).getId())
        and:
        entities.get(0).getVersion() == 1
        and:
        entities.get(0).getValue() == "SELECT"
        when:
        entities = elastic.select(QueryTestEntity.class).
                eq(QueryTestEntity.VALUE, "SELECT").
                orderAsc(QueryTestEntity.COUNTER).
                skip(10).
                limit(10).
                queryList()
        then:
        entities.size() == 10
        and:
        entities.get(0).getCounter() == 10
        and:
        entities.get(9).getCounter() == 19
    }

    def "aggregations work"() {
        when:
        for (int i = 0; i < 100; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("AGG" + (i % 10))
            entity.setCounter(i)
            elastic.update(entity)
        }
        Wait.seconds(2)
        def query = elastic.select(QueryTestEntity.class).
                addTermAggregation(QueryTestEntity.VALUE).
                filter(new Prefix(QueryTestEntity.VALUE, "AGG"))
        def entities = query.queryList()
        def buckets = query.getAggregationBuckets(QueryTestEntity.VALUE.toString())
        then:
        entities.size() == 100
        and:
        buckets.size() == 10
        and:
        buckets.get(0).getSecond() == 10
    }

    def "count works"() {
        when:
        for (int i = 0; i < 100; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("COUNT")
            elastic.update(entity)
        }
        Wait.seconds(2)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "COUNT").count() == 100
    }

    def "exists works"() {
        when:
        for (int i = 0; i < 10; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("EXISTS")
            elastic.update(entity)
        }
        Wait.seconds(2)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "EXISTS").exists()
    }

    def "scroll query works"() {
        when:
        for (int i = 1; i <= 1500; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("SCROLL")
            entity.setCounter(i)
            elastic.update(entity)
        }
        Wait.seconds(2)
        and:
        int sum = 0
        elastic.select(QueryTestEntity.class).
                eq(QueryTestEntity.VALUE, "SCROLL").
                iterateAll({ e -> sum += e.getCounter() })
        then:
        sum == (1500 * 1501) / 2
    }

}
