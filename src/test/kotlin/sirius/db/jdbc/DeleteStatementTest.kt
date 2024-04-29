/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@ExtendWith(SiriusExtension::class)
class DeleteStatementTest {
    @Test
    fun `a delete statement deletes the expected entities`() {
        val generatedStatementTestEntity1 = GeneratedStatementTestEntity()
        generatedStatementTestEntity1.testNumber = 4711
        generatedStatementTestEntity1.value = "2"
        oma.update(generatedStatementTestEntity1)

        val generatedStatementTestEntity2 = GeneratedStatementTestEntity()
        generatedStatementTestEntity2.testNumber = 4712
        generatedStatementTestEntity2.value = "4"
        oma.update(generatedStatementTestEntity2)

        val changes = oma.deleteStatement(
                GeneratedStatementTestEntity::class.java
        ).where(GeneratedStatementTestEntity.TEST_NUMBER, 4711)
                .executeUpdate()

        assertEquals(1, changes)
        assertEquals(
                Optional.empty(),
                oma.find(GeneratedStatementTestEntity::class.java, generatedStatementTestEntity1.getId())
        )
        assertNotNull(oma.find(GeneratedStatementTestEntity::class.java, generatedStatementTestEntity2.getId()))
    }


    @Test
    fun `a delete statement deletes all entities without errors`() {
        val generatedStatementTestEntity = GeneratedStatementTestEntity()
        generatedStatementTestEntity.testNumber = 4711
        generatedStatementTestEntity.value = "2"
        oma.update(generatedStatementTestEntity)
        oma.deleteStatement(
                GeneratedStatementTestEntity::class.java
        ).executeUpdate()

        assertEquals(0, oma.select(GeneratedStatementTestEntity::class.java).count())
    }

    companion object {
        @Part
        private lateinit var oma: OMA

    }
}
