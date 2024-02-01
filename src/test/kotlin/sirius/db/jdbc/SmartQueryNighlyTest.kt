/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.Tags
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

@ExtendWith(SiriusExtension::class)
@Tag(Tags.NIGHTLY)
class SmartQueryNightlyTest {
    @Test
    fun `selecting over 1000 entities in queryList throws an exception`() {
        oma.select(ListTestEntity::class.java).delete()

        for (i in 0..1000) {
            val entityToCreate = ListTestEntity()
            entityToCreate.counter = i
            oma.update(entityToCreate)
        }

        assertThrows<HandledException> { oma.select(ListTestEntity::class.java).queryList() }
    }

    companion object {
        @Part
        private lateinit var oma: OMA
    }
}
