/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.jdbc.OMA
import sirius.db.jdbc.SQLEntity
import sirius.db.jdbc.TestEntity
import sirius.db.mixing.Mixing
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull

@ExtendWith(SiriusExtension::class)
class BatchContextTest {
    @Test
    fun `insert works`() {
        val ctbatchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val insert = ctbatchContext.insertQuery(
                TestEntity::class.java,
                TestEntity.FIRSTNAME,
                TestEntity.LASTNAME,
                TestEntity.AGE
        )
        for (i in 0..99) {
            val testEntity = TestEntity()
            testEntity.firstname = "BatchContextInsert" + i
            testEntity.lastname = "INSERT"
            insert.insert(testEntity, true, false)
        }

        assertEquals(100, oma.select(TestEntity::class.java).eq(TestEntity.LASTNAME, "INSERT").count())

        OMA.LOG.INFO(ctbatchContext)
        ctbatchContext.close()
    }

    @Test
    fun `batch insert works`() {
        val batchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val insert = batchContext.insertQuery(
                TestEntity::class.java,
                TestEntity.FIRSTNAME,
                TestEntity.LASTNAME,
                TestEntity.AGE
        )
        for (i in 0..99) {
            val testEntity = TestEntity()
            testEntity.firstname = "BatchContextInsert" + i
            testEntity.lastname = "BATCHINSERT"
            insert.insert(testEntity, false, true)
        }

        assertEquals(0, oma.select(TestEntity::class.java).eq(TestEntity.LASTNAME, "BATCHINSERT").count())

        insert.commit()

        assertEquals(100, oma.select(TestEntity::class.java).eq(TestEntity.LASTNAME, "BATCHINSERT").count())

        OMA.LOG.INFO(batchContext)
        batchContext.close()
    }

    @Test
    fun `update works`() {
        val testEntity = TestEntity()
        testEntity.firstname = "Updating"
        testEntity.lastname = "Test"
        oma.update(testEntity)
        val batchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val update = batchContext.updateByIdQuery(TestEntity::class.java, TestEntity.FIRSTNAME)
        testEntity.firstname = "Updated"
        update.update(testEntity, true, false)

        assertEquals("Updated", oma.refreshOrFail(testEntity).firstname)

        OMA.LOG.INFO(batchContext)
        batchContext.close()
    }

    @Test
    fun `batch update works`() {
        val testEntity = TestEntity()
        testEntity.firstname = "Updating"
        testEntity.lastname = "Test"
        oma.update(testEntity)
        val ctx = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val update = ctx.updateByIdQuery(TestEntity::class.java, TestEntity.FIRSTNAME)
        testEntity.firstname = "Updated"
        update.update(testEntity, true, true)

        assertNotEquals("Updated", oma.refreshOrFail(testEntity).firstname)

        update.commit()

        assertEquals("Updated", oma.refreshOrFail(testEntity).firstname)

        OMA.LOG.INFO(ctx)
        ctx.close()
    }

    @Test
    fun `find works`() {
        val testEntity = TestEntity()
        testEntity.firstname = "BatchContextFind"
        testEntity.lastname = "Test"
        oma.update(testEntity)
        val batchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val find = batchContext.findQuery(TestEntity::class.java, TestEntity.FIRSTNAME)
        val found = TestEntity()
        found.firstname = "BatchContextFind"
        found.lastname = "Test"
        val notFound = TestEntity()
        notFound.firstname = "BatchContextFind2"
        found.lastname = "Test"

        assertEquals(testEntity, find.find(found).get())
        assertNotNull(find.find(found))
        assertFalse { find.find(found).get().isNew }
        assertNotNull(find.find(notFound))

        OMA.LOG.INFO(batchContext)
        batchContext.close()
    }


    @Test
    fun `delete works`() {
        val testEntity = TestEntity()
        testEntity.firstname = "Delete"
        testEntity.lastname = "Test"
        oma.update(testEntity)
        val batchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val delete = batchContext.deleteQuery(TestEntity::class.java, TestEntity.FIRSTNAME)
        delete.delete(testEntity, true, false)

        assertNotNull(oma.resolve<SQLEntity>(testEntity.uniqueName))

        OMA.LOG.INFO(batchContext)
        batchContext.close()
    }

    @Test
    fun `delete in batch works`() {
        val testEntity = TestEntity()
        testEntity.firstname = "BatchDelete"
        testEntity.lastname = "Test"
        oma.update(testEntity)
        val batchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val delete = batchContext.deleteQuery(TestEntity::class.java, TestEntity.FIRSTNAME)
        delete.delete(testEntity, true, true)

        assertNotNull(oma.resolve<SQLEntity>(testEntity.getUniqueName()))

        delete.commit()

        assertNotNull(oma.resolve<SQLEntity>(testEntity.getUniqueName()))

        OMA.LOG.INFO(batchContext)
        batchContext.close()
    }

    @Test
    fun `custom query works`() {
        val testEntity = TestEntity()
        testEntity.firstname = "BatchContextFind"
        testEntity.lastname = "CustomTest"
        oma.update(testEntity)
        val batchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val find =
                batchContext.customQuery(TestEntity::class.java, false, "SELECT * FROM testentity WHERE lastname = ?")
        find.setParameter(1, "CustomTest")

        assertEquals("CustomTest", find.query().queryFirst()!!.getValue("LASTNAME").asString())

        find.setParameter(1, "CustomTestXXX")

        assertNotNull(find.query.first())

        OMA.LOG.INFO(batchContext)
        batchContext.close()
    }

    @Test
    fun `use after close is prevented`() {
        val batchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        batchContext.close()

        assertThrows<IllegalStateException> {
            batchContext.insertQuery(
                    TestEntity::class.java,
                    TestEntity.FIRSTNAME,
                    TestEntity.LASTNAME,
                    TestEntity.AGE
            )
        }
        assertEquals(0, oma.getDatabase(Mixing.DEFAULT_REALM)!!.getNumActive())
    }

    @Test
    fun `use of query after close is prevented`() {
        val batchContext = BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        val insert = batchContext.insertQuery(
                TestEntity::class.java,
                TestEntity.FIRSTNAME,
                TestEntity.LASTNAME,
                TestEntity.AGE
        )
        batchContext.close()

        assertThrows<HandledException> { insert.insert(TestEntity(), false, true) }

        assertEquals(0, oma.getDatabase(Mixing.DEFAULT_REALM)!!.getNumActive())
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            oma.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
