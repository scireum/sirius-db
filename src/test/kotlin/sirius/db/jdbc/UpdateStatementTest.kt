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
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class UpdateStatementTest {
    @Test
    fun `an update statement updates the expected entities`() {
        val generatedStatementTestEntity1 = GeneratedStatementTestEntity()
        generatedStatementTestEntity1.testNumber = 1
        generatedStatementTestEntity1.value = "2"
        oma.update(generatedStatementTestEntity1)

        val generatedStatementTestEntity2 = GeneratedStatementTestEntity()
        generatedStatementTestEntity2.testNumber = 3
        generatedStatementTestEntity2.value = "4"
        oma.update(generatedStatementTestEntity2)

        val changes = oma.updateStatement(
                GeneratedStatementTestEntity::class.java
        ).set(GeneratedStatementTestEntity.VALUE, "5")
                .where(GeneratedStatementTestEntity.TEST_NUMBER, 1).executeUpdate()

        assertEquals(1, changes)
        assertEquals("5", oma.refreshOrFail(generatedStatementTestEntity1).value)
        assertEquals("4", oma.refreshOrFail(generatedStatementTestEntity2).value)
    }

    @Test
    fun `an update statement reports illegal use (set after where)`() {
        assertThrows<IllegalStateException> {
            oma.updateStatement(
                    GeneratedStatementTestEntity::class.java
            ).where(GeneratedStatementTestEntity.TEST_NUMBER, 1)
                    .set(GeneratedStatementTestEntity.VALUE, "5").executeUpdate()
        }
    }

    @Test
    fun `an update statement ignores an empty update without errors`() {
        val changes = oma.updateStatement(GeneratedStatementTestEntity::class.java).where(
                GeneratedStatementTestEntity.TEST_NUMBER,
                1
        ).executeUpdate()

        assertEquals(0, changes)
    }

    @Test
    fun `an update statement detects and reports join columns as errors`() {
        assertThrows<IllegalArgumentException> {
            oma.updateStatement(
                    SmartQueryTestChildEntity::class.java
            ).set(SmartQueryTestChildEntity.NAME, "X")
                    .where(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME), 1)
                    .executeUpdate()
        }
    }

    companion object {
        @Part
        private lateinit var oma: OMA
    }
}
