/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es


import sirius.db.es.properties.ESStringListEntity
import sirius.db.es.properties.ESStringMapEntity
import sirius.db.mixing.Mapping
import sirius.db.mixing.properties.StringMapProperty
import sirius.kernel.BaseSpecification
import sirius.kernel.Scope
import sirius.kernel.commons.Doubles
import sirius.kernel.commons.Strings
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

import java.time.Duration
import java.time.LocalDateTime

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
        elastic.refresh(QueryTestEntity.class)
        def entities = elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "SELECT").queryList()
        then:
        entities.size() == 100
        and:
        Strings.isFilled(entities.get(0).getId())
        and:
        entities.get(0).getSeqNo() == 0
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

    def "sorting works"() {
        when:
        for (int i = 0; i < 100; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("SORT")
            entity.setCounter(i)
            elastic.update(entity)
        }
        elastic.refresh(QueryTestEntity.class)
        def entities = elastic.select(QueryTestEntity.class)
                              .eq(QueryTestEntity.VALUE, "SORT")
                              .sort(SortBuilder.on(QueryTestEntity.COUNTER)
                                               .order(SortBuilder.Order.DESC))
                              .queryList()
        then:
        entities.size() == 100
        and:
        entities.get(0).getCounter() == 99
        and:
        entities.get(99).getCounter() == 0
    }

    def "aggregations work"() {
        when:
        for (int i = 0; i < 100; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("AGG" + (i % 10))
            entity.setCounter(i)
            elastic.update(entity)
        }
        elastic.refresh(QueryTestEntity.class)
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
        elastic.refresh(ESStringMapEntity.class)
        def subAggregation = AggregationBuilder.create(AggregationBuilder.TERMS, "keys")
                                               .field(ESStringMapEntity.MAP.nested(Mapping.
                                                       named(StringMapProperty.KEY)))
        def aggregation = AggregationBuilder.createNested(ESStringMapEntity.MAP, "test")
                                            .addSubAggregation(subAggregation)
        def query = elastic.select(ESStringMapEntity.class)
                           .eq(ESStringMapEntity.ID, entity.getId())
                           .addAggregation(aggregation)
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

    def "muli-level nested aggregations work"() {
        given:
        def keysAggregation = AggregationBuilder.create(AggregationBuilder.TERMS, "keys")
                                                .field(ESStringMapEntity.MAP.nested(Mapping.named(StringMapProperty.
                                                        VALUE)))
        def filterAggregation = AggregationBuilder.createFiltered("filter", Elastic.FILTERS.eq(ESStringMapEntity.
                MAP.nested(Mapping.named(StringMapProperty.KEY)), "3")).addSubAggregation(keysAggregation)
        when:
        ESStringMapEntity entity = new ESStringMapEntity()
        entity.getMap().put("3", "3").put("4", "4").put("test2", "test2")
        elastic.update(entity)
        elastic.refresh(ESStringMapEntity.class)
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
        elastic.refresh(QueryTestEntity.class)
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
        elastic.refresh(QueryTestEntity.class)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "DELETE").count() == 100
        when:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "DELETE").delete()
        and:
        elastic.refresh(QueryTestEntity.class)
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
        elastic.refresh(QueryTestEntity.class)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "TRUNCATE").count() == 100
        when:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "TRUNCATE").truncate()
        and:
        elastic.refresh(QueryTestEntity.class)
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
        elastic.refresh(QueryTestEntity.class)
        then:
        elastic.select(QueryTestEntity.class).eq(QueryTestEntity.VALUE, "EXISTS").exists()
    }

    @Scope(Scope.SCOPE_NIGHTLY)
    def "scroll query works"() {
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
    }

    def "queries with multiple occurences of the same constraint works"() {
        when:
        QueryTestEntity entity = new QueryTestEntity()
        entity.setValue("NOREF")
        entity.setCounter(1)
        elastic.update(entity)
        elastic.refresh(QueryTestEntity.class)
        and:
        def constraint = Elastic.FILTERS.eq(QueryTestEntity.VALUE, "NOREF")
        and:
        def entities = elastic.
                select(QueryTestEntity.class).
                where(Elastic.FILTERS.or(constraint, constraint)).
                queryList()
        then:
        entities.size() == 1
    }

    def "containsAny query works"() {
        setup:
        ESStringListEntity entity = new ESStringListEntity()
        entity.getList().modify().addAll(["1", "2", "3"])
        ESStringListEntity entityEmpty = new ESStringListEntity()
        when:
        elastic.update(entity)
        elastic.update(entityEmpty)
        elastic.refresh(ESStringListEntity.class)
        then:
        elastic.select(ESStringListEntity.class)
               .eq(ESStringListEntity.ID, entity.getId())
               .where(Elastic.FILTERS.containsAny(ESStringListEntity.LIST, Value.of("2,4,5")).build())
               .queryOne().getId() == entity.getId()
        then:
        elastic.select(ESStringListEntity.class)
               .eq(ESStringListEntity.ID, entity.getId())
               .where(Elastic.FILTERS.containsAny(ESStringListEntity.LIST, Value.of("4,5,6")).build())
               .count() == 0
        then:
        elastic.select(ESStringListEntity.class)
               .eq(ESStringListEntity.ID, entityEmpty.getId())
               .where(Elastic.FILTERS.containsAny(ESStringListEntity.LIST, Value.of("4,5,6")).orEmpty().build())
               .queryOne().getId() == entityEmpty.getId()
    }

    def "field value score queries work"() {
        when:
        for (int i = 0; i < 100; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("FUNCTIONSCORE")
            entity.setCounter(i)
            elastic.update(entity)
        }
        elastic.refresh(QueryTestEntity.class)
        and:
        def query = elastic.select(QueryTestEntity.class)
                           .must(Elastic.FILTERS.eq(QueryTestEntity.VALUE, "FUNCTIONSCORE"))
                           .functionScore(new FunctionScoreBuilder().fieldValueFuncion(QueryTestEntity.COUNTER, 2, 1)
                                                                    .parameter("boost_mode", "replace"))
                           .orderByScoreAsc()
        and:
        def entities = query.queryList()
        then:
        entities.size() == 100
        and:
        entities.get(0).getScore() == 0
        and:
        entities.get(50).getScore() == 100
        and:
        entities.get(99).getScore() == 198
    }

    def "decay score works"() {
        when:
        for (int i = 1; i <= 30; i++) {
            QueryTestEntity entity = new QueryTestEntity()
            entity.setValue("DECAYSCORE")
            entity.setDateTime(LocalDateTime.of(2020, 06, i, 12, 0, 0))
            elastic.update(entity)
        }
        elastic.refresh(QueryTestEntity.class)
        def origin = LocalDateTime.of(2020, 06, 30, 12, 0, 0)
        def functionScore = new FunctionScoreBuilder().linearDateTimeDecayFunction(QueryTestEntity.DATE_TIME,
                                                                                   origin,
                                                                                   Duration.ofDays(9),
                                                                                   Duration.ofDays(10),
                                                                                   0.5f)
        functionScore.parameter("boost_mode", "replace")
        and:
        def query = elastic.select(QueryTestEntity.class)
                           .must(Elastic.FILTERS.eq(QueryTestEntity.VALUE, "DECAYSCORE"))
                           .functionScore(functionScore)
                           .orderByScoreDesc()
        and:
        def entities = query.queryList()
        then:
        entities.size() == 30
        and:
        Doubles.areEqual(entities.get(0).getScore(), 1d)
        and:
        Doubles.areEqual(entities.get(9).getScore(), 1d)
        and:
        Doubles.areEqual(entities.get(19).getScore(), 0.5d)
        and:
        Doubles.areEqual(entities.get(29).getScore(), 0d)
    }

    @Scope(Scope.SCOPE_NIGHTLY)
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
