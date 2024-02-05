/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mixing.Mixing
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.Duration
import java.util.stream.Collectors
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class TransformedQueryTest {
    @Test
    fun `transform works when reading a test entity`() {
        val sqlQuery = oma.getDatabase(Mixing.DEFAULT_REALM)
                ?.createQuery("SELECT * FROM transformedquerytestentity ORDER BY value ASC")
        val testEntity = oma.transform(TransformedQueryTestEntity::class.java, sqlQuery).queryFirst()
        val testEntities = oma.transform(TransformedQueryTestEntity::class.java, sqlQuery).queryList()

        assertEquals("Hello", testEntity.value)
        assertEquals(
                listOf("Hello", "Test", "World"),
                testEntities.stream().map { x -> x.value }.collect(Collectors.toList())
        )
    }

    @Test
    fun `transform works when reading a test entity with alias`() {
        val sqlQuery = oma.getDatabase(Mixing.DEFAULT_REALM)
                ?.createQuery("SELECT id as x_id, value as x_value  FROM transformedquerytestentity ORDER BY value ASC")
        val testEntity = oma.transform(TransformedQueryTestEntity::class.java, "x", sqlQuery).first()

        assertTrue { testEntity.isPresent }
        assertEquals("Hello", testEntity.get().value)
    }

    @Test
    fun `transform works when reading a test entity with a computed column`() {
        val sqlQuery = oma.getDatabase(Mixing.DEFAULT_REALM)
                ?.createQuery("SELECT id, value, 'x' as test FROM transformedquerytestentity ORDER BY value ASC")
        val testEntity = oma.transform(TransformedQueryTestEntity::class.java, sqlQuery).queryFirst()

        assertEquals("Hello", testEntity.value)
        assertEquals("x", testEntity.getFetchRow().getValue("test").asString())
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            oma.readyFuture.await(Duration.ofSeconds(60))
            var transformedQueryTestEntity = TransformedQueryTestEntity()
            transformedQueryTestEntity.value = "Test"
            oma.update(transformedQueryTestEntity)
            transformedQueryTestEntity = TransformedQueryTestEntity()
            transformedQueryTestEntity.value = "Hello"
            oma.update(transformedQueryTestEntity)
            transformedQueryTestEntity = TransformedQueryTestEntity()
            transformedQueryTestEntity.value = "World"
            oma.update(transformedQueryTestEntity)
        }
    }
}
