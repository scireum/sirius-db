/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class ReindexTest {
    @Test
    fun `reindex and move alias works`() {
        val elasticTestEntity = ElasticTestEntity()
        elasticTestEntity.age = 10
        elasticTestEntity.firstname = "test"
        elasticTestEntity.lastname = "test"
        elastic.update(elasticTestEntity)
        elastic.refresh(ElasticTestEntity::class.java)
        elastic.getLowLevelClient()
                .startReindex(elastic.determineReadAlias(elasticTestEntity.descriptor), "reindex-test")
        Wait.seconds((2).toDouble())

        assertTrue { elastic.getLowLevelClient().indexExists("reindex-test") }

        elastic.getLowLevelClient()
                .createOrMoveAlias(elastic.determineReadAlias(elasticTestEntity.descriptor), "reindex-test")
        val indicesForAlias = elastic.determineEffectiveIndex(elasticTestEntity.descriptor)

        assertEquals("reindex-test", indicesForAlias)
        assertNotNull(elastic.find(ElasticTestEntity::class.java, elasticTestEntity.id))
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
