/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.Elastic
import sirius.db.mongo.Mango
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.Duration
import kotlin.test.assertFalse

@ExtendWith(SiriusExtension::class)
class BaseEntityRefListTest {
    @Test
    fun `cascade from Mongo to ES works`() {
        val refMongoEntity = RefListMongoEntity()
        mango.update(refMongoEntity)
        val refElasticEntity = RefListElasticEntity()
        refElasticEntity.ref.add(refMongoEntity.id)
        elastic.update(refElasticEntity)
        elastic.refresh(RefListElasticEntity::class.java)
        mango.delete(refMongoEntity)
        elastic.refresh(RefListElasticEntity::class.java)

        assertFalse { (elastic.find(RefListElasticEntity::class.java, refElasticEntity.id).isPresent) }
    }

    @Test
    fun `cascade from ES to Mongo works`() {
        val refElasticEntity = RefListElasticEntity()
        elastic.update(refElasticEntity)
        elastic.refresh(RefListElasticEntity::class.java)
        val refMongoEntity = RefListMongoEntity()
        refMongoEntity.ref.add(refElasticEntity.id)
        mango.update(refMongoEntity)
        elastic.delete(refElasticEntity)
        elastic.refresh(RefListElasticEntity::class.java)
        val resolved = mango.refreshOrFail(refMongoEntity)

        assertFalse { resolved.ref.contains(refElasticEntity.id) }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic

        @Part
        private lateinit var mango: Mango

        fun setupSpec() {
            elastic.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
