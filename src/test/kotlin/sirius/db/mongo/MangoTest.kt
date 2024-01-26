/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mixing.IntegrityConstraintFailedException
import sirius.db.mixing.OptimisticLockException
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class MangoTest {
    companion object {
        @Part
        private lateinit var mango: Mango
    }

    @Test
    fun `write a test entity and read it back`() {
        val mangoTestEntity = MangoTestEntity()
        mangoTestEntity.firstname = "Test"
        mangoTestEntity.lastname = "Entity"
        mangoTestEntity.age = 12

        mango.update(mangoTestEntity)

        val readBack = mango.findOrFail(MangoTestEntity::class.java, mangoTestEntity.getId())

        assertEquals("Test", readBack.firstname)
        assertEquals("Entity", readBack.lastname)
        assertEquals(12, readBack.age)
    }

    @Test
    fun `select not all fields`() {
        val mangoTestEntity = MangoTestEntity()
        mangoTestEntity.firstname = "Test2"
        mangoTestEntity.lastname = "Entity2"
        mangoTestEntity.age = 13

        mango.update(mangoTestEntity)

        val readBack = mango.select(MangoTestEntity::class.java)
                .eq(MangoTestEntity.ID, mangoTestEntity.getId())
                .fields(MangoTestEntity.FIRSTNAME, MangoTestEntity.AGE)
                .queryFirst()

        assertNotEquals(null, readBack)
        assertFalse {
            readBack.descriptor.isFetched(readBack, readBack.descriptor.getProperty(MangoTestEntity.ID))
        }
        assertTrue {
            readBack.descriptor
                    .isFetched(readBack, readBack.descriptor.getProperty(MangoTestEntity.FIRSTNAME))
        }
        assertFalse {
            readBack.descriptor.isFetched(readBack, readBack.descriptor.getProperty(MangoTestEntity.LASTNAME))
        }
        assertTrue {
            readBack.descriptor.isFetched(readBack, readBack.descriptor.getProperty(MangoTestEntity.AGE))
        }
        assertEquals("Test2", readBack.firstname)
        assertEquals(null, readBack.lastname)
        assertEquals(13, readBack.age)
    }

    @Test
    fun `delete an entity`() {
        val mangoTestEntity = MangoTestEntity()
        mangoTestEntity.firstname = "Test"
        mangoTestEntity.lastname = "Entity"
        mangoTestEntity.age = 12

        mango.update(mangoTestEntity)
        val refreshed = mango.tryRefresh(mangoTestEntity)
        mango.delete(mangoTestEntity)

        assertThrows<HandledException> {
            mango.refreshOrFail(mangoTestEntity)

            // The first refresh worked
            assertEquals(mangoTestEntity, refreshed)
            // The second refresh failed as expected
            refreshed != mangoTestEntity
        }
    }

    @Test
    fun `optimistic locking works`() {
        val mongoLockedTestEntity = MongoLockedTestEntity()
        mongoLockedTestEntity.value = "Test"
        mango.update(mongoLockedTestEntity)

        val copyOfOriginal = mango.refreshOrFail(mongoLockedTestEntity)

        mongoLockedTestEntity.value = "Test2"
        mango.update(mongoLockedTestEntity)

        mongoLockedTestEntity.value = "Test3"
        mango.update(mongoLockedTestEntity)

        copyOfOriginal.value = "Test2"

        assertThrows<OptimisticLockException> { mango.tryUpdate(copyOfOriginal) }
        assertThrows<OptimisticLockException> { mango.tryDelete(copyOfOriginal) }

        mango.forceDelete(copyOfOriginal)
        val notFound = mango.find(MongoLockedTestEntity::class.java, mongoLockedTestEntity.getId()).orElse(null)

        assertEquals(null, notFound)
    }

    @Test
    fun `unique constraint violations are properly thrown`() {
        mango.select(
                MongoUniqueTestEntity::class.java
        ).eq(MongoUniqueTestEntity.VALUE, "Test").delete()

        val entity = MongoUniqueTestEntity()
        entity.value = "Test"
        mango.update(entity)

        val conflictingEntity = MongoUniqueTestEntity()

        conflictingEntity.value = "Test"
        assertThrows<IntegrityConstraintFailedException> { mango.tryUpdate(conflictingEntity) }
    }

    @Test
    fun `MongoQuery exists works as expected and leaves the query intact`() {
        mango.select(
                MangoListTestEntity::class.java
        ).delete()

        for (i in 0..9) {
            val entityToCreate = MangoListTestEntity()
            entityToCreate.counter = i
            mango.update(entityToCreate)
        }

        val query = mango.select(
                MangoListTestEntity::class.java
        ).orderDesc(MangoListTestEntity.COUNTER)

        //simple exists works
        assertTrue { query.exists() }
        //a count after an exists still yields all entities
        assertEquals(10, query.count())
        //a list after an exists still yields all entities
        assertEquals(10, query.queryList().size)
        //a list after an exists still yields all fields
        assertEquals(9, query.queryList()[0].counter)
        //an exists with a filter also works
        assertTrue { mango.select(MangoListTestEntity::class.java).eq(MangoListTestEntity.COUNTER, 5).exists() }
        //an exists with a filter that yields an empty result works
        assertFalse { mango.select(MangoListTestEntity::class.java).eq(MangoListTestEntity.COUNTER, 50).exists() }
    }

    @Test
    fun `MongoQuery streamBlockwise() works in mango`() {
        mango.select(
                MangoListTestEntity::class.java
        ).delete()

        for (i in 0..9) {
            val entityToCreate = MangoListTestEntity()
            entityToCreate.counter = i
            mango.update(entityToCreate)
        }

        val query = mango.select(MangoListTestEntity::class.java)

        assertThrows<UnsupportedOperationException> {
            assertEquals(10, query.streamBlockwise().count())
            assertEquals(7, query.skip(3).limit(0).streamBlockwise().count())
        }
    }

    @Test
    fun `wasCreated() works in mango`() {
        val testEntity = MangoWasCreatedTestEntity()
        testEntity.value = "test123"

        mango.update(testEntity)
        assertTrue { testEntity.hasJustBeenCreated() }

        mango.update(testEntity)
        assertFalse { testEntity.hasJustBeenCreated() }
    }

    @Test
    fun `a forcefully failed query does not yield any results`() {
        mango.select(
                MangoListTestEntity::class.java
        ).delete()

        for (i in 0..2) {
            val entityToCreate = MangoListTestEntity()
            entityToCreate.counter = i
            mango.update(entityToCreate)
        }

        val query = mango.select(MangoListTestEntity::class.java).fail()
        var flag = false

        assertTrue { query.queryList().isEmpty() }

        query.iterateAll { flag = true }

        assertFalse { flag }
        assertEquals(0, query.count())
        assertFalse { query.exists() }
    }

    @Test
    fun `simple aggregations work`() {

        val mango1 = MangoAggregationsTestEntity()
        mango1.testInt = 30
        mango.update(mango1)
        val mango2 = MangoAggregationsTestEntity()
        mango2.testInt = 10
        mango.update(mango2)
        val mango3 = MangoAggregationsTestEntity()
        mango3.testInt = 20
        mango.update(mango3)

        assertEquals(
                60, mango.select(
                MangoAggregationsTestEntity::class.java
        ).aggregateSum(MangoAggregationsTestEntity.TEST_INT).asInt(0)
        )
        assertEquals(
                20.0,
                mango.select(MangoAggregationsTestEntity::class.java)
                        .aggregateAverage(MangoAggregationsTestEntity.TEST_INT)
                        .asDouble(0.0)
        )
        assertEquals(
                10,
                mango.select(MangoAggregationsTestEntity::class.java).aggregateMin(MangoAggregationsTestEntity.TEST_INT)
                        .asInt(0)
        )
        assertEquals(
                30,
                mango.select(MangoAggregationsTestEntity::class.java).aggregateMax(MangoAggregationsTestEntity.TEST_INT)
                        .asInt(0)
        )
    }
}
