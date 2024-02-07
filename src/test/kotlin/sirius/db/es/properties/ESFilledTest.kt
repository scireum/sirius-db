/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.Elastic
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class ESFilledTest {
    @Test
    fun `filled, notFilled and exists query works`() {
        val fieldFilled = ESFilledEntity()
        fieldFilled.testField = "test"
        val fieldNotFilled = ESFilledEntity()
        elastic.update(fieldFilled)
        elastic.update(fieldNotFilled)
        elastic.refresh(ESFilledEntity::class.java)

        assertEquals(
                fieldNotFilled.idAsString,
                elastic.select(ESFilledEntity::class.java).eq(ESFilledEntity.TEST_FIELD, null).queryFirst().idAsString
        )
        assertEquals(1, elastic.select(ESFilledEntity::class.java).eq(ESFilledEntity.TEST_FIELD, null).count())
        assertEquals(
                fieldNotFilled.idAsString,
                elastic.select(ESFilledEntity::class.java).where(Elastic.FILTERS.notFilled(ESFilledEntity.TEST_FIELD))
                        .queryFirst().idAsString
        )
        assertEquals(
                1,
                elastic.select(ESFilledEntity::class.java).where(Elastic.FILTERS.notFilled(ESFilledEntity.TEST_FIELD))
                        .count()
        )
        assertEquals(
                fieldFilled.idAsString,
                elastic.select(ESFilledEntity::class.java).ne(ESFilledEntity.TEST_FIELD, null).queryFirst().idAsString
        )
        assertEquals(1, elastic.select(ESFilledEntity::class.java).ne(ESFilledEntity.TEST_FIELD, null).count())
        assertEquals(
                fieldFilled.idAsString,
                elastic.select(ESFilledEntity::class.java).where(Elastic.FILTERS.filled(ESFilledEntity.TEST_FIELD))
                        .queryFirst().idAsString
        )
        assertEquals(
                1,
                elastic.select(ESFilledEntity::class.java).where(Elastic.FILTERS.filled(ESFilledEntity.TEST_FIELD))
                        .count()
        )
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
