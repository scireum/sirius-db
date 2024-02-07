/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.properties.ESStringListEntity
import sirius.db.es.properties.ESStringMapEntity
import sirius.db.mixing.Mapping
import sirius.db.mixing.properties.StringMapProperty
import sirius.db.mongo.Mango
import sirius.db.mongo.MangoTestEntity
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Doubles
import sirius.kernel.commons.Strings
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import java.time.Duration
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class ElasticQueryTest {
    @Test
    fun `queryList works`() {
        for (i in 0..99) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "SELECT"
            queryTestEntity.counter = i
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)
        var entities = elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "SELECT").queryList()

        assertEquals(100, entities.size)
        assertEquals("SELECT", entities.get(0).value)
        assertTrue { Strings.isFilled(entities.get(0).id) }

        entities = elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "SELECT")
                .orderAsc(QueryTestEntity.COUNTER).skip(10).limit(10).queryList()

        assertEquals(10, entities.size)
        assertEquals(10, entities[0].counter)
        assertEquals(19, entities[9].counter)
    }

    @Test
    fun `streamBlockwise works`() {
        for (i in 0..9) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "STREAM"
            queryTestEntity.counter = i
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)

        assertEquals(
                10,
                elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "STREAM").streamBlockwise()
                        .count()
        )

        assertThrows<UnsupportedOperationException> {
            elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "STREAM")
                    .skip(5).limit(6).streamBlockwise().count()
        }
    }

    @Test
    fun `sorting works`() {
        for (i in 0..99) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "SORT"
            queryTestEntity.counter = i
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)
        val entities = elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "SORT")
                .orderBy(SortBuilder.on(QueryTestEntity.COUNTER).order(SortBuilder.Order.DESC)).queryList()

        assertEquals(100, entities.size)
        assertEquals(99, entities[0].counter)
        assertEquals(0, entities[99].counter)
    }

    @Test
    fun `aggregations work`() {
        for (i in 0..99) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "AGG" + (i % 10)
            queryTestEntity.counter = i
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)
        val query = elastic.select(QueryTestEntity::class.java).addTermAggregation(QueryTestEntity.VALUE)
                .where(Elastic.FILTERS.prefix(QueryTestEntity.VALUE, "AGG"))
        val entities = query.queryList()
        val buckets = query.getAggregation(QueryTestEntity.VALUE.toString()).buckets

        assertEquals(100, entities.size)
        assertEquals(10, buckets.size)
        assertEquals(10, buckets.get(0).docCount)
    }

    @Test
    fun `nested aggregations work`() {
        val esStringMapEntity = ESStringMapEntity()
        esStringMapEntity.map.put("1", "1").put("2", "2").put("test", "test")
        elastic.update(esStringMapEntity)
        elastic.refresh(ESStringMapEntity::class.java)
        val subAggregation = AggregationBuilder.create(AggregationBuilder.TERMS, "keys")
                .field(ESStringMapEntity.MAP.nested(Mapping.named(StringMapProperty.KEY)))
        val aggregation =
                AggregationBuilder.createNested(ESStringMapEntity.MAP, "test").addSubAggregation(subAggregation)
        val query = elastic.select(ESStringMapEntity::class.java).eq(ESStringMapEntity.ID, esStringMapEntity.id)
                .addAggregation(aggregation)
        query.computeAggregations()
        val buckets = query.rawAggregations.withArray("/test/keys/buckets")

        assertEquals(3, buckets.size())
        assertEquals("1", buckets.get(0).path("key").asText())
        assertEquals("2", buckets.get(1).path("key").asText())
        assertEquals("test", buckets.get(2).path("key").asText())
    }

    @Test
    fun `muli-level nested aggregations work`() {
        val keysAggregation = AggregationBuilder.create(AggregationBuilder.TERMS, "keys")
                .field(ESStringMapEntity.MAP.nested(Mapping.named(StringMapProperty.VALUE)))
        val filterAggregation = AggregationBuilder.createFiltered(
                "filter",
                Elastic.FILTERS.eq(ESStringMapEntity.MAP.nested(Mapping.named(StringMapProperty.KEY)), "3")
        ).addSubAggregation(keysAggregation)
        val esStringMapEntity = ESStringMapEntity()
        esStringMapEntity.map.put("3", "3").put("4", "4").put("test2", "test2")
        elastic.update(esStringMapEntity)
        elastic.refresh(ESStringMapEntity::class.java)
        val query = elastic.select(ESStringMapEntity::class.java)
                .eq(ESStringMapEntity.ID, esStringMapEntity.id)
                .addAggregation(
                        AggregationBuilder.createNested(ESStringMapEntity.MAP, "test")
                                .addSubAggregation(filterAggregation)
                )
        query.computeAggregations()
        val buckets = query.rawAggregations.withArray("/test/filter/keys/buckets")

        assertEquals(1, buckets.size())
        assertEquals("3", buckets.get(0).path("key").asText())
    }

    @Test
    fun `count works`() {
        for (i in 0..99) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "COUNT"
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)

        assertEquals(100, elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "COUNT").count())
    }

    @Test
    fun `delete works`() {
        for (i in 0..99) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "DELETE"
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)

        assertEquals(100, elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "DELETE").count())

        elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "DELETE").delete()
        elastic.refresh(QueryTestEntity::class.java)

        assertEquals(0, elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "DELETE").count())
    }

    @Test
    fun `truncate works`() {
        for (i in 0..99) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "TRUNCATE"
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)

        assertEquals(100, elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "TRUNCATE").count())

        elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "TRUNCATE").truncate()
        elastic.refresh(QueryTestEntity::class.java)

        assertEquals(0, elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "TRUNCATE").count())
    }

    @Test
    fun `exists works`() {
        for (i in 0..9) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "EXISTS"
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)

        assertTrue { elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "EXISTS").exists() }
    }

    @Test
    fun `queries with multiple occurences of the same constraint works`() {
        val queryTestEntity = QueryTestEntity()
        queryTestEntity.value = "NOREF"
        queryTestEntity.counter = 1
        elastic.update(queryTestEntity)
        elastic.refresh(QueryTestEntity::class.java)
        val constraint = Elastic.FILTERS.eq(QueryTestEntity.VALUE, "NOREF")
        val entities = elastic.select(QueryTestEntity::class.java).where(Elastic.FILTERS.or(constraint, constraint))
                .queryList()

        assertEquals(1, entities.size)
    }

    @Test
    fun `containsAny query works`() {
        val esStringListEntity = ESStringListEntity()
        esStringListEntity.list.modify().addAll(listOf("1", "2", "3"))
        val esStringListEntityEmpty = ESStringListEntity()
        elastic.update(esStringListEntity)
        elastic.update(esStringListEntityEmpty)
        elastic.refresh(ESStringListEntity::class.java)

        assertEquals(
                esStringListEntity.id,
                elastic.select(ESStringListEntity::class.java).eq(ESStringListEntity.ID, esStringListEntity.id)
                        .where(Elastic.FILTERS.containsAny(ESStringListEntity.LIST, Value.of("2,4,5")).build())
                        .queryOne().id
        )
        assertEquals(
                0,
                elastic.select(ESStringListEntity::class.java).eq(ESStringListEntity.ID, esStringListEntity.id)
                        .where(Elastic.FILTERS.containsAny(ESStringListEntity.LIST, Value.of("4,5,6")).build()).count()
        )
        assertEquals(
                esStringListEntityEmpty.id,
                elastic.select(ESStringListEntity::class.java).eq(ESStringListEntity.ID, esStringListEntityEmpty.id)
                        .where(
                                Elastic.FILTERS.containsAny(ESStringListEntity.LIST, Value.of("4,5,6")).orEmpty()
                                        .build()
                        ).queryOne().id
        )
    }

    @Test
    fun `allInField query works`() {
        val esStringListEntity = ESStringListEntity()
        esStringListEntity.list.modify().addAll(listOf("1", "2", "3"))
        elastic.update(esStringListEntity)
        elastic.refresh(ESStringListEntity::class.java)

        assertEquals(
                0,
                elastic.select(ESStringListEntity::class.java).eq(ESStringListEntity.ID, esStringListEntity.id)
                        .where(Elastic.FILTERS.allInField(ESStringListEntity.LIST, listOf("1", "2", "3", "4"))).count()
        )
        assertEquals(
                esStringListEntity.id,
                elastic.select(ESStringListEntity::class.java).eq(ESStringListEntity.ID, esStringListEntity.id)
                        .where(Elastic.FILTERS.allInField(ESStringListEntity.LIST, listOf("1", "2", "3"))).queryOne().id
        )
        assertEquals(
                esStringListEntity.id,
                elastic.select(ESStringListEntity::class.java).eq(ESStringListEntity.ID, esStringListEntity.id)
                        .where(Elastic.FILTERS.allInField(ESStringListEntity.LIST, listOf("1", "2"))).queryOne().id
        )
    }

    @Test
    fun `namedOr query and match detection works`() {
        val queryTestEntity1 = QueryTestEntity()
        queryTestEntity1.value = "NAMED-OR"
        queryTestEntity1.counter = 1
        elastic.update(queryTestEntity1)
        val queryTestEntity2 = QueryTestEntity()
        queryTestEntity2.value = "NAMED-OR"
        queryTestEntity2.counter = 2
        elastic.update(queryTestEntity2)
        elastic.refresh(QueryTestEntity::class.java)
        val result = elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "NAMED-OR").where(
                Elastic.FILTERS.namedOr(
                        "namedOr",
                        Elastic.FILTERS.eq(QueryTestEntity.COUNTER, 1),
                        Elastic.FILTERS.eq(QueryTestEntity.COUNTER, 2)
                )
        ).queryList()

        assertEquals(2, result.size)
        assertTrue { result.get(0).isMatchedNamedQuery("namedOr") }
    }

    @Test
    fun `field value score queries work`() {
        for (i in 0..99) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "FUNCTIONSCORE"
            queryTestEntity.counter = i
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)
        val elasticQuery = elastic.select(QueryTestEntity::class.java)
                .must(Elastic.FILTERS.eq(QueryTestEntity.VALUE, "FUNCTIONSCORE")).functionScore(
                        FunctionScoreBuilder().fieldValueFunction(QueryTestEntity.COUNTER, 2F, 1F)
                                .parameter("boost_mode", "replace")
                ).orderByScoreAsc()
        val entities = elasticQuery.queryList()

        assertEquals(100, entities.size)
        assertEquals(0F, entities.get(0).score)
        assertEquals(100F, entities.get(50).getScore())
        assertEquals(198F, entities.get(99).getScore())
    }

    @Test
    fun `decay score works`() {
        for (i in 1..30) {
            val queryTestEntity = QueryTestEntity()
            queryTestEntity.value = "DECAYSCORE"
            queryTestEntity.dateTime = LocalDateTime.of(2020, 6, i, 12, 0, 0)
            elastic.update(queryTestEntity)
        }
        elastic.refresh(QueryTestEntity::class.java)
        val origin = LocalDateTime.of(2020, 6, 30, 12, 0, 0)
        val functionScore = FunctionScoreBuilder().linearDateTimeDecayFunction(
                QueryTestEntity.DATE_TIME,
                origin,
                Duration.ofDays(9),
                Duration.ofDays(10),
                0.5f
        )
        functionScore.parameter("boost_mode", "replace")
        val elasticQuery = elastic.select(QueryTestEntity::class.java)
                .must(Elastic.FILTERS.eq(QueryTestEntity.VALUE, "DECAYSCORE")).functionScore(functionScore)
                .orderByScoreDesc()
        val entities = elasticQuery.queryList()

        assertEquals(30, entities.size)

        assertTrue { Doubles.areEqual(entities[0].getScore().toDouble(), (1).toDouble()) }
        assertTrue { Doubles.areEqual(entities[9].getScore().toDouble(), (1).toDouble()) }
        assertTrue { Doubles.areEqual(entities[19].getScore().toDouble(), 0.5) }
        assertTrue { Doubles.areEqual(entities[29].getScore().toDouble(), (0).toDouble()) }
    }

    @Test
    fun `a forcefully failed query does not yield any results`() {
        elastic.select(ESListTestEntity::class.java).delete()
        for (i in 0..2) {
            val entityToCreate = ESListTestEntity()
            entityToCreate.counter = i
            elastic.update(entityToCreate)
        }
        elastic.refresh(ESListTestEntity::class.java)
        val elasticQuery = elastic.select(ESListTestEntity::class.java).fail()
        var flag = false

        assertTrue { elasticQuery.queryList().isEmpty() }

        elasticQuery.iterateAll { e -> flag = true }

        assertFalse { flag }
        assertEquals(0, elasticQuery.count())
        assertFalse { elasticQuery.exists() }
    }

    @Test
    fun `search for a mongo reference works`() {
        val mangoTestEntity = MangoTestEntity()
        mangoTestEntity.firstname = "Compiler"
        mangoTestEntity.lastname = "Test"
        mango.update(mangoTestEntity)
        val elasticTestEntity = QueryTestEntity()
        elasticTestEntity.mongoId.setValue(mangoTestEntity)
        elasticTestEntity.counter = 10
        elasticTestEntity.value = "Test123"
        elastic.update(elasticTestEntity)
        elastic.refresh(QueryTestEntity::class.java)
        val elasticTestEntityRecoveredViaConstraint =
                elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.MONGO_ID, mangoTestEntity.id).queryOne()
        val elasticTestEntityRecoveredViaQueryString = elastic.select(QueryTestEntity::class.java).where(
                elastic.filters().queryString(
                        elasticTestEntity.descriptor,
                        QueryTestEntity.MONGO_ID.name + ":" + mangoTestEntity.id
                )
        ).queryOne()

        assertNotNull(elasticTestEntityRecoveredViaConstraint)
        assertEquals(10, elasticTestEntityRecoveredViaConstraint.counter)
        assertEquals("Test123", elasticTestEntityRecoveredViaConstraint.value)
        assertEquals(mangoTestEntity.id, elasticTestEntityRecoveredViaConstraint.mongoId.id)

        assertNotNull(elasticTestEntityRecoveredViaQueryString)
        assertEquals(10, elasticTestEntityRecoveredViaQueryString.counter)
        assertEquals("Test123", elasticTestEntityRecoveredViaQueryString.value)
        assertEquals(mangoTestEntity.id, elasticTestEntityRecoveredViaQueryString.mongoId.id)
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic

        @Part
        private lateinit var mango: Mango

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            elastic.getReadyFuture().await(Duration.ofSeconds(60))
        }
    }
}
