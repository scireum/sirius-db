/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import sirius.db.es.properties.ESStringMapEntity
import sirius.db.mixing.Mapping
import sirius.db.mixing.properties.StringMapProperty
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
        def query = elastic.select(QueryTestEntity.class)
                           .addTermAggregation(QueryTestEntity.VALUE)
                           .where(Elastic.FILTERS.prefix(QueryTestEntity.VALUE, "AGG"))
        def entities = query.queryList()
        def buckets = query.getAggregationBuckets(QueryTestEntity.VALUE.toString())
        then:
        entities.size() == 100
        and:
        buckets.size() == 10
        and:
        buckets.get(0).getSecond() == 10
    }

    def "nested aggregations work"() {
        when:
        ESStringMapEntity entity = new ESStringMapEntity()
        entity.getMap().put("1", "1").put("2", "2").put("test", "test")
        elastic.update(entity)
        Wait.seconds(1)
        def query = elastic.select(ESStringMapEntity.class)
                           .eq(ESStringMapEntity.ID, entity.getId())
                           .addAggregation(AggregationBuilder.createNested(ESStringMapEntity.MAP, "test")
                           .addSubAggregation(AggregationBuilder.create(AggregationBuilder.TERMS, "keys")
                           .field(ESStringMapEntity.MAP.nested(Mapping.named(StringMapProperty.KEY)))))
        query.computeAggregations()
        then:
        query.getRawAggregations().getJSONObject("test")
             .getJSONObject("keys").getJSONArray("buckets").size() == 3
        query.getRawAggregations().getJSONObject("test")
             .getJSONObject("keys").getJSONArray("buckets").getJSONObject(0)
             .getString("key") == "1"
        query.getRawAggregations().getJSONObject("test")
             .getJSONObject("keys").getJSONArray("buckets").getJSONObject(1)
             .getString("key") == "2"
        query.getRawAggregations().getJSONObject("test")
             .getJSONObject("keys").getJSONArray("buckets").getJSONObject(2)
             .getString("key") == "test"
    }

    def "muli-level nested aggregations work"(){
        given:
        def keysAggregation = AggregationBuilder.create(AggregationBuilder.TERMS, "keys")
                                                .field(ESStringMapEntity.MAP.nested(Mapping.named(StringMapProperty.VALUE)))
        def filterAggregation = AggregationBuilder.createFiltered("filter", Elastic.FILTERS.eq(ESStringMapEntity.MAP.nested(Mapping.named(StringMapProperty.KEY)), "3"))
                                                  .addSubAggregation(keysAggregation)
        when:
        ESStringMapEntity entity = new ESStringMapEntity()
        entity.getMap().put("3", "3").put("4", "4").put("test2", "test2")
        elastic.update(entity)
        Wait.seconds(1)
        def query = elastic.select(ESStringMapEntity.class)
                           .eq(ESStringMapEntity.ID, entity.getId())
                           .addAggregation(AggregationBuilder.createNested(ESStringMapEntity.MAP, "test")
                                                         .addSubAggregation(filterAggregation))
        query.computeAggregations()
        then:
        query.getRawAggregations().getJSONObject("test")
             .getJSONObject("filter").getJSONObject("keys")
             .getJSONArray("buckets").size() == 1
        query.getRawAggregations().getJSONObject("test")
             .getJSONObject("filter").getJSONObject("keys")
             .getJSONArray("buckets").getJSONObject(0)
             .getString("key") == "3"

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

    def "delete works"() {
        when:
        for (int i = 0; i < 100; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("DELETE")
            elastic.update(entity)
        }
        Wait.seconds(2)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "DELETE").count() == 100
        when:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "DELETE").delete()
        and:
        Wait.seconds(2)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "DELETE").count() == 0
    }

    def "truncate works"() {
        when:
        for (int i = 0; i < 100; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("TRUNCATE")
            elastic.update(entity)
        }
        Wait.seconds(2)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "TRUNCATE").count() == 100
        when:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "TRUNCATE").truncate()
        and:
        Wait.seconds(2)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "TRUNCATE").count() == 0
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

    def "queries with multiple occurences of the same constraint works"() {
        when:
        QueryTestEntity entity = new QueryTestEntity()
        entity.setValue("NOREF")
        entity.setCounter(1)
        elastic.update(entity)
        Wait.seconds(2)
        and:
        def constraint = Elastic.FILTERS.eq(QueryTestEntity.VALUE, "NOREF")
        and:
        def entities = elastic.select(QueryTestEntity.class).where(Elastic.FILTERS.or(constraint, constraint)).queryList()
        then:
        entities.size() == 1
    }
}
