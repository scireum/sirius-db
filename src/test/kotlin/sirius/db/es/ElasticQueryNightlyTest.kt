/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.Tags
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import java.time.Duration
import kotlin.test.assertEquals

@Tag(Tags.NIGHTLY)
@ExtendWith(SiriusExtension::class)
class ElasticQueryNightlyTest {
    @Test
    fun `scroll query and large streamBlockwise works`() {
        for (i in 1..1499) {
            val entity = QueryTestEntity()
            entity.value = "SCROLL"
            entity.counter = i
            elastic.update(entity)
        }
        elastic.refresh(QueryTestEntity::class.java)
        var sum = 0
        elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "SCROLL")
                .iterateAll { e -> sum += e.counter }

        assertEquals((1500 * 1501) / 2, sum)
        assertEquals(
                sum,
                elastic.select(QueryTestEntity::class.java).eq(QueryTestEntity.VALUE, "SCROLL").streamBlockwise()
                        .mapToInt { e -> e.counter }.sum()
        )
    }

    @Test
    fun `selecting over 1000 entities in queryList throws an exception`() {
        elastic.select(ESListTestEntity::class.java).delete()
        for (i in 0..1000) {
            val entityToCreate = ESListTestEntity()
            entityToCreate.counter = i
            elastic.update(entityToCreate)
        }
        elastic.refresh(ESListTestEntity::class.java)

        assertThrows<HandledException> { elastic.select(ESListTestEntity::class.java).queryList() }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            elastic.getReadyFuture().await(Duration.ofSeconds(60))
        }
    }
}
