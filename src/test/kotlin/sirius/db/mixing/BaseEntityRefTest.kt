/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.Elastic
import sirius.db.jdbc.OMA
import sirius.db.mongo.Mango
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import java.time.Duration
import kotlin.test.assertFalse

@ExtendWith(SiriusExtension::class)
class BaseEntityRefTest {
    @Test
    fun `cascade from JDBC to ES and Mongo works`() {
        val refEntity = RefEntity()
        oma.update(refEntity)
        val refElasticEntity = RefElasticEntity()
        refElasticEntity.ref.setValue(refEntity)
        elastic.update(refElasticEntity)
        elastic.refresh(RefElasticEntity::class.java)
        val refMongoEntity = RefMongoEntity()
        refMongoEntity.ref.setValue(refEntity)
        mango.update(refMongoEntity)
        oma.delete(refEntity)
        elastic.refresh(RefElasticEntity::class.java)

        assertFalse { elastic.find(RefElasticEntity::class.java, refElasticEntity.id).isPresent }
        assertFalse { mango.find(RefMongoEntity::class.java, refMongoEntity.id).isPresent }
    }

    @Test
    fun `cascade from ES to JDBC works`() {
        val refElasticEntity = RefElasticEntity()
        elastic.update(refElasticEntity)
        val refEntity = RefEntity()
        refEntity.elastic.setValue(refElasticEntity)
        oma.update(refEntity)
        elastic.delete(refElasticEntity)

        assertFalse { oma.find(RefEntity::class.java, refEntity.id).isPresent }
    }

    @Test
    fun `cascade from Mongo to JDBC works`() {
        val refMongoEntity = RefMongoEntity()
        mango.update(refMongoEntity)

        val refEntity = RefEntity()
        refEntity.mongo.setValue(refMongoEntity)
        oma.update(refEntity)

        mango.delete(refMongoEntity)

        assertFalse { oma.find(RefEntity::class.java, refEntity.id).isPresent }
    }

    @Test
    fun `writeOnce semantics are enforced`() {
        assertThrows<HandledException> {
            val writeOnceParentEntity1 = WriteOnceParentEntity()
            oma.update(writeOnceParentEntity1)

            val writeOnceChildEntity = WriteOnceChildEntity()
            writeOnceChildEntity.parent.setValue(writeOnceParentEntity1)
            oma.update(writeOnceChildEntity)

            val writeOnceParentEntity2 = WriteOnceParentEntity()
            oma.update(writeOnceParentEntity2)

            writeOnceChildEntity.parent.setValue(writeOnceParentEntity2)
            oma.update(writeOnceChildEntity)
        }
    }

    @Test
    fun `writeOnce semantics permit a non-changing update`() {
        assertDoesNotThrow {
            val writeOnceParentEntity = WriteOnceParentEntity()
            oma.update(writeOnceParentEntity)

            val writeOnceChildEntity = WriteOnceChildEntity()
            writeOnceChildEntity.parent.setValue(writeOnceParentEntity)
            oma.update(writeOnceChildEntity)

            writeOnceChildEntity.parent.setValue(writeOnceParentEntity)
            oma.update(writeOnceChildEntity)
        }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic

        @Part
        private lateinit var oma: OMA

        @Part
        private lateinit var mango: Mango

        fun setupSpec() {
            elastic.readyFuture.await(Duration.ofSeconds(60))
            oma.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
