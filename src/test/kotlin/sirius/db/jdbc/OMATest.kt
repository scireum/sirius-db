/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mixing.IntegrityConstraintFailedException
import sirius.db.mixing.OptimisticLockException
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.Duration
import kotlin.test.*

@ExtendWith(SiriusExtension::class)
class OMATest {
    @Test
    fun `write a test entity and read it back`() {
        val testEntity = TestEntity()
        testEntity.firstname = "Test"
        testEntity.lastname = "Entity"
        testEntity.age = 12
        oma.update(testEntity)

        val readBack = oma.findOrFail(TestEntity::class.java, testEntity.getId())

        assertEquals("Test", readBack.firstname)
        assertEquals("Entity", readBack.lastname)
        assertEquals(12, readBack.age)
    }

    @Test
    fun `write and read an entity with composite`() {
        val testEntityWithComposite = TestEntityWithComposite()
        testEntityWithComposite.composite.city = "x"
        testEntityWithComposite.composite.street = "y"
        testEntityWithComposite.composite.zip = "z"
        testEntityWithComposite.compositeWithComposite.composite.city = "a"
        testEntityWithComposite.compositeWithComposite.composite.street = "b"
        testEntityWithComposite.compositeWithComposite.composite.zip = "c"
        oma.update(testEntityWithComposite)

        val readBack = oma.findOrFail(TestEntityWithComposite::class.java, testEntityWithComposite.getId())

        assertEquals("x", readBack.composite.city)
        assertEquals("y", readBack.composite.street)
        assertEquals("z", readBack.composite.zip)
        assertEquals("a", readBack.compositeWithComposite.composite.city)
        assertEquals("b", readBack.compositeWithComposite.composite.street)
        assertEquals("c", readBack.compositeWithComposite.composite.zip)
    }

    @Test
    fun `write and read an entity with mixin`() {
        val testEntityWithMixin = TestEntityWithMixin()
        testEntityWithMixin.firstname = "Homer"
        testEntityWithMixin.lastname = "Simpson"
        testEntityWithMixin.`as`(TestMixin::class.java).middleName = "Jay"
        testEntityWithMixin.`as`(TestMixin::class.java).`as`(TestMixinMixin::class.java).initial = "J"
        oma.update(testEntityWithMixin)

        val readBack = oma.findOrFail(TestEntityWithMixin::class.java, testEntityWithMixin.getId())

        assertEquals("Homer", readBack.firstname)
        assertEquals("Simpson", readBack.lastname)
        assertEquals("Jay", readBack.`as`(TestMixin::class.java).middleName)
        assertEquals("J", readBack.`as`(TestMixin::class.java).`as`(TestMixinMixin::class.java).initial)
    }

    @Test
    fun `select from secondary works`() {
        val testEntity = TestEntity()
        testEntity.firstname = "Marge"
        testEntity.lastname = "Simpson"
        testEntity.age = 43
        oma.update(testEntity)

        val readBack = oma.selectFromSecondary(TestEntity::class.java)
                .eq(TestEntity.ID, testEntity.getId())
                .queryFirst()

        assertNotNull(readBack)
        assertEquals("Marge", readBack.firstname)
        assertEquals("Simpson", readBack.lastname)
        assertEquals(43, readBack.age)
    }

    @Test
    fun `select not all fields`() {
        val testEntity = TestEntity()
        testEntity.firstname = "Marge"
        testEntity.lastname = "Simpson"
        testEntity.age = 43
        oma.update(testEntity)

        val readBack = oma.select(TestEntity::class.java)
                .eq(TestEntity.ID, testEntity.getId())
                .fields(TestEntity.FIRSTNAME, TestEntity.AGE)
                .queryFirst()

        assertNotNull(readBack)
        assertFalse { readBack.descriptor.isFetched(readBack, readBack.descriptor.getProperty(TestEntity.ID)) }
        assertTrue { readBack.descriptor.isFetched(readBack, readBack.descriptor.getProperty(TestEntity.FIRSTNAME)) }
        assertFalse { readBack.descriptor.isFetched(readBack, readBack.descriptor.getProperty(TestEntity.LASTNAME)) }
        assertTrue { readBack.descriptor.isFetched(readBack, readBack.descriptor.getProperty(TestEntity.AGE)) }
        assertEquals("Marge", readBack.firstname)
        assertNull(readBack.lastname)
        assertEquals(43, readBack.age)
    }

    @Test
    fun `resolve can resolve an entity by its unique name`() {
        val testClobEntity = TestClobEntity()
        testClobEntity.largeValue = "test"
        oma.update(testClobEntity)

        assertEquals(oma.resolveOrFail(testClobEntity.getUniqueName()), testClobEntity)
    }

    @Test
    fun `change tracking works`() {
        val testEntityWithMixin = TestEntityWithMixin()
        testEntityWithMixin.firstname = "Homer"
        testEntityWithMixin.lastname = "Simpson"
        testEntityWithMixin.`as`(TestMixin::class.java).middleName = "Jay"
        testEntityWithMixin.`as`(TestMixin::class.java).`as`(TestMixinMixin::class.java).initial = "J"

        assertTrue {
            testEntityWithMixin.isAnyMappingChanged
            testEntityWithMixin.isChanged(TestEntityWithMixin.FIRSTNAME)
        }

        oma.update(testEntityWithMixin)

        assertFalse { testEntityWithMixin.isAnyMappingChanged }

        testEntityWithMixin.lastname = "SimpsonSimpson"

        assertTrue {
            testEntityWithMixin.isAnyMappingChanged
            testEntityWithMixin.isChanged(TestEntityWithMixin.LASTNAME)
        }

        oma.update(testEntityWithMixin)

        testEntityWithMixin.`as`(TestMixin::class.java).middleName = "JayJay"

        assertTrue {
            testEntityWithMixin.isAnyMappingChanged
            testEntityWithMixin.isChanged(TestMixin.MIDDLE_NAME.inMixin(TestMixin::class.java))
        }
    }

    @Test
    fun `optimistic locking works`() {
        val sqlLockedTestEntity = SQLLockedTestEntity()
        sqlLockedTestEntity.value = "Test"
        oma.update(sqlLockedTestEntity)

        val copyOfOriginal = oma.refreshOrFail(sqlLockedTestEntity)

        assertThrows<OptimisticLockException> {
            sqlLockedTestEntity.value = "Test2"
            oma.update(sqlLockedTestEntity)

            sqlLockedTestEntity.value = "Test3"
            oma.update(sqlLockedTestEntity)

            copyOfOriginal.value = "Test2"
            oma.tryUpdate(copyOfOriginal)

            oma.tryDelete(copyOfOriginal)
        }

        oma.forceDelete(copyOfOriginal)
        val notFound = oma.find(SQLLockedTestEntity::class.java, sqlLockedTestEntity.getId()).orElse(null)

        assertNull(notFound)
    }

    @Test
    fun `unique constraint violations are properly thrown`() {
        oma.select(SQLUniqueTestEntity::class.java).eq(SQLUniqueTestEntity.VALUE, "Test").delete()

        assertThrows<IntegrityConstraintFailedException> {
            val sqlUniqueTestEntity = SQLUniqueTestEntity()
            sqlUniqueTestEntity.value = "Test"
            oma.update(sqlUniqueTestEntity)

            val conflictingEntity = SQLUniqueTestEntity()

            conflictingEntity.value = "Test"
            oma.tryUpdate(conflictingEntity)
        }
    }

    @Test
    fun `wasCreated() works in OMA`() {
        val sqlWasCreatedTestEntity = SQLWasCreatedTestEntity()
        sqlWasCreatedTestEntity.value = "test123"

        oma.update(sqlWasCreatedTestEntity)

        assertTrue { sqlWasCreatedTestEntity.hasJustBeenCreated() }

        oma.update(sqlWasCreatedTestEntity)

        assertFalse { sqlWasCreatedTestEntity.hasJustBeenCreated() }
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        fun setupSpec() {
            oma.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
