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
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mixing.Mixing
import sirius.db.mixing.OptimisticLockException
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.Duration
import kotlin.test.*

@ExtendWith(SiriusExtension::class)
class ElasticTest {
    @Test
    fun `update, find and delete works`() {
        val elasticTestEntity = ElasticTestEntity()
        elasticTestEntity.firstname = "Hello"
        elasticTestEntity.lastname = "World"
        elasticTestEntity.age = 12
        elastic.update(elasticTestEntity)
        elastic.refresh(ElasticTestEntity::class.java)
        val loaded = elastic.findOrFail(ElasticTestEntity::class.java, elasticTestEntity.id)

        assertEquals("Hello", loaded.firstname)
        assertEquals("World", loaded.lastname)
        assertEquals(12, loaded.age)

        elastic.delete(elasticTestEntity)
        elastic.refresh(ElasticTestEntity::class.java)
        val notFound = elastic.find(ElasticTestEntity::class.java, elasticTestEntity.id).orElse(null)

        assertNull(notFound)
    }

    @Test
    fun `update, find and delete works with routing`() {
        val routedTestEntity = RoutedTestEntity()
        routedTestEntity.firstname = "Hello"
        routedTestEntity.lastname = "World"
        routedTestEntity.age = 12
        elastic.update(routedTestEntity)
        elastic.refresh(RoutedTestEntity::class.java)
        val loaded = elastic.findOrFail(RoutedTestEntity::class.java, routedTestEntity.id, Elastic.routedBy("World"))
        val notLoaded =
                elastic.find(RoutedTestEntity::class.java, routedTestEntity.id, Elastic.routedBy("XX_badRouting"))
                        .orElse(null)

        assertEquals("Hello", loaded.firstname)
        assertEquals("World", loaded.lastname)
        assertEquals(12, loaded.age)
        assertNull(notLoaded)

        val refreshed = elastic.refreshOrFail(routedTestEntity)

        assertEquals("Hello", refreshed.firstname)

        elastic.delete(routedTestEntity)
        elastic.refresh(RoutedTestEntity::class.java)
        val notFound =
                elastic.find(RoutedTestEntity::class.java, routedTestEntity.id, Elastic.routedBy("World")).orElse(null)

        assertNull(notFound)
    }

    /**
     * Note that this test only ensures that suppressing the routing works properly.
     * <p>
     * Production code should never mix routed and unrouted access on an entity and expect this to work
     * (Normally this is also rejected and reported by the framework, unless the <tt>elasticsearch.suppressedRoutings</tt>
     * is used).
     */
    @Test
    fun `update, find and delete works with suppressed routing`() {
        val suppressedRoutedTestEntity = SuppressedRoutedTestEntity()
        suppressedRoutedTestEntity.firstname = "Hello"
        suppressedRoutedTestEntity.lastname = "World"
        suppressedRoutedTestEntity.age = 12
        elastic.update(suppressedRoutedTestEntity)
        elastic.refresh(SuppressedRoutedTestEntity::class.java)
        val loaded = elastic.findOrFail(
                SuppressedRoutedTestEntity::class.java,
                suppressedRoutedTestEntity.id,
                Elastic.routedBy("World")
        )
        val alsoLoaded = elastic.find(
                SuppressedRoutedTestEntity::class.java,
                suppressedRoutedTestEntity.id,
                Elastic.routedBy("XX_badRouting")
        ).orElse(null)

        assertEquals("Hello", loaded.firstname)
        assertEquals("World", loaded.lastname)
        assertEquals(12, loaded.age)
        assertEquals("Hello", alsoLoaded.firstname)
        assertEquals("World", alsoLoaded.lastname)
        assertEquals(12, alsoLoaded.age)

        elastic.select(SuppressedRoutedTestEntity::class.java).routing("World")
                .eq(SuppressedRoutedTestEntity.LASTNAME, "World").exists()
        elastic.select(SuppressedRoutedTestEntity::class.java).eq(SuppressedRoutedTestEntity.LASTNAME, "World").exists()
        elastic.select(SuppressedRoutedTestEntity::class.java).routing("XXX")
                .eq(SuppressedRoutedTestEntity.LASTNAME, "World").exists()
        val refreshed = elastic.refreshOrFail(suppressedRoutedTestEntity)

        assertEquals("Hello", refreshed.firstname)

        elastic.delete(suppressedRoutedTestEntity)
        elastic.refresh(RoutedTestEntity::class.java)
        val notFound =
                elastic.find(RoutedTestEntity::class.java, suppressedRoutedTestEntity.id, Elastic.routedBy("World"))
                        .orElse(null)

        assertNull(notFound)
    }

    @Test
    fun `optimistic locking works`() {
        val lockedTestEntity = LockedTestEntity()
        lockedTestEntity.value = "Test"
        elastic.update(lockedTestEntity)
        elastic.refresh(LockedTestEntity::class.java)
        val copyOfOriginal = elastic.refreshOrFail(lockedTestEntity)

        assertThrows<OptimisticLockException> {
            lockedTestEntity.value = "Test2"
            elastic.update(lockedTestEntity)
            elastic.refresh(LockedTestEntity::class.java)

            lockedTestEntity.value = "Test3"
            elastic.update(lockedTestEntity)
            elastic.refresh(LockedTestEntity::class.java)

            copyOfOriginal.value = "Test2"
            elastic.tryUpdate(copyOfOriginal)
        }
        assertThrows<OptimisticLockException> { elastic.tryDelete(copyOfOriginal) }

        elastic.forceDelete(copyOfOriginal)
        elastic.refresh(LockedTestEntity::class.java)
        val notFound = elastic.find(LockedTestEntity::class.java, lockedTestEntity.id).orElse(null)

        assertNull(notFound)
    }

    @Test
    fun `wasCreated() works in elastic`() {
        val elasticWasCreatedTestEntity = ElasticWasCreatedTestEntity()
        elasticWasCreatedTestEntity.value = "test123"
        elastic.update(elasticWasCreatedTestEntity)

        assertTrue { elasticWasCreatedTestEntity.hasJustBeenCreated() }

        elastic.update(elasticWasCreatedTestEntity)

        assertFalse { elasticWasCreatedTestEntity.hasJustBeenCreated() }
    }

    @Test
    fun `Deleting a non-existing entity simply does nothing`() {
        val elasticTestEntity = ElasticTestEntity()
        elasticTestEntity.id = "DOES_NOT_EXIST"

        assertDoesNotThrow { elastic.delete(elasticTestEntity) }
    }

    @Test
    fun `Custom write index works`() {
        val testEntity = ElasticTestEntity()
        testEntity.firstname = "Test"
        testEntity.lastname = "Entity"
        testEntity.age = 12
        elastic.update(testEntity)
        elastic.refresh(ElasticTestEntity::class.java)
        elastic.createAndInstallWriteIndex(mixing.getDescriptor(ElasticTestEntity::class.java))
        elastic.refresh(ElasticTestEntity::class.java)
        elastic.delete(testEntity)
        elastic.refresh(ElasticTestEntity::class.java)

        assertNotNull(elastic.find(ElasticTestEntity::class.java, testEntity.id))

        val secondTestEntity = ElasticTestEntity()
        secondTestEntity.firstname = "Second"
        secondTestEntity.lastname = "Entity"
        secondTestEntity.age = 13
        elastic.update(secondTestEntity)
        elastic.refresh(ElasticTestEntity::class.java)

        assertNotNull(elastic.find(ElasticTestEntity::class.java, secondTestEntity.id))

        elastic.commitWriteIndex(mixing.getDescriptor(ElasticTestEntity::class.java))

        assertTrue { elastic.find(ElasticTestEntity::class.java, secondTestEntity.id).isPresent }

        elastic.delete(secondTestEntity)
        elastic.refresh(ElasticTestEntity::class.java)

        assertNotNull(elastic.find(ElasticTestEntity::class.java, testEntity.id))
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic

        @Part
        private lateinit var mixing: Mixing

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            elastic.getReadyFuture().await(Duration.ofSeconds(60))
        }
    }
}
