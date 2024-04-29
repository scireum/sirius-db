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
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class BulkContextTest {
    @Test
    fun `batch insert works`() {
        val bulkContext = elastic.batch()
        elastic.select(BatchTestEntity::class.java).delete()
        bulkContext.tryUpdate(BatchTestEntity().withValue(1))
        bulkContext.tryUpdate(BatchTestEntity().withValue(2))
        bulkContext.tryUpdate(BatchTestEntity().withValue(3))
        bulkContext.commit()
        elastic.refresh(BatchTestEntity::class.java)

        assertEquals(3, elastic.select(BatchTestEntity::class.java).count())
    }

    @Test
    fun `batch insert with routing works`() {
        val bulkContext = elastic.batch()
        elastic.select(RoutedBatchTestEntity::class.java).delete()
        bulkContext.tryUpdate(RoutedBatchTestEntity().withValue(1).withValue1(5))
        bulkContext.tryUpdate(RoutedBatchTestEntity().withValue(2).withValue1(5))
        bulkContext.tryUpdate(RoutedBatchTestEntity().withValue(3).withValue1(5))
        bulkContext.commit()
        elastic.refresh(RoutedBatchTestEntity::class.java)

        assertEquals(3,
                elastic.select(RoutedBatchTestEntity::class.java).routing("5").eq(RoutedBatchTestEntity.VALUE1, 5)
                        .count()
        )
    }

    @Test
    fun `optimistic locking with batchcontext works`() {
        val bulkContext = elastic.batch()
        elastic.select(BatchTestEntity::class.java).delete()
        val modified = BatchTestEntity().withValue(100)
        elastic.update(modified)
        elastic.refresh(BatchTestEntity::class.java)
        val original = elastic.refreshOrFail(modified)
        elastic.update(modified.withValue(200))
        elastic.refresh(BatchTestEntity::class.java)
        bulkContext.tryUpdate(original.withValue(150))
        bulkContext.commit()
        elastic.refresh(BatchTestEntity::class.java)

        assertEquals(200, elastic.refreshOrFail(original).value)
    }

    @Test
    fun `overwriting with batchcontext works`() {
        val bulkContext = elastic.batch()
        elastic.select(BatchTestEntity::class.java).delete()
        val modified = BatchTestEntity().withValue(100)
        elastic.update(modified)
        elastic.refresh(BatchTestEntity::class.java)
        val original = elastic.refreshOrFail(modified)
        elastic.update(modified.withValue(200))
        elastic.refresh(BatchTestEntity::class.java)
        bulkContext.overwrite(original.withValue(150))
        bulkContext.commit()
        elastic.refresh(BatchTestEntity::class.java)

        assertEquals(150, elastic.refreshOrFail(original).value)
    }

    @Test
    fun `delete with batchcontext works`() {
        val bulkContext = elastic.batch()
        elastic.select(BatchTestEntity::class.java).delete()
        val batchTestEntity = BatchTestEntity ().withValue(100)
        elastic.update(batchTestEntity)
        elastic.refresh(BatchTestEntity::class.java)
        bulkContext.tryDelete(batchTestEntity)
        bulkContext.commit()
        elastic.refresh(BatchTestEntity::class.java)

        assertNotNull(elastic.find(BatchTestEntity::class.java, batchTestEntity.id))
    }

    @Test
    fun `beforeSave in bulkContext works`() {
        val bulkContext = elastic.batch()
        val elasticTestEntity = ElasticTestEntity ()
        elasticTestEntity.firstname = null

        assertThrows<HandledException> { bulkContext.tryUpdate(elasticTestEntity) }
    }

    @Test
    fun `getFailedIds() works`() {
        val bulkContext = elastic.batch()
        elastic.select(BatchTestEntity::class.java).delete()
        val batchTestEntity1 = BatchTestEntity ().withValue(1)
        val batchTestEntity2 = BatchTestEntity ().withValue(10)
        elastic.update(batchTestEntity1)
        val refreshed = elastic . refreshOrFail (batchTestEntity1)
        elastic.update(batchTestEntity1.withValue(2))
        val result = bulkContext . tryUpdate (refreshed.withValue(3)).tryUpdate(batchTestEntity2).commit()

        assertFalse { result.isSuccessful }
        assertEquals(1,result.failedIds.size)
        assertTrue { result.getFailedIds().contains(refreshed.id) }
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
