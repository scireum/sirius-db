/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.Tags
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import java.util.*
import kotlin.test.assertEquals

@Tag(Tags.NIGHTLY)
@ExtendWith(SiriusExtension::class)
class MangoNightlyTest {
    companion object {
        @Part
        private lateinit var mango: Mango
    }

    @Test
    fun `selecting over 1000 entities in queryList throws an exception`() {
        mango.select(MangoListTestEntity::class.java).delete()
        for (i in 0..1000) {
            val entityToCreate = MangoListTestEntity()
            entityToCreate.counter = i
            mango.update(entityToCreate)
        }

        assertThrows<HandledException> { mango.select(MangoListTestEntity::class.java).queryList() }
    }

    @Test
    fun `a timed out mongo count returns an empty optional`() {
        mango.select(
                MangoListTestEntity::class.java
        ).delete()

        for (i in 0..99_999) {
            val entityToCreate = MangoListTestEntity()
            entityToCreate.counter = i
            mango.update(entityToCreate)
        }
        val query = mango
                .select(
                        MangoListTestEntity::class.java
                )

        assertEquals(
                Optional.empty(), query.count(
                true, 1
        )
        )
    }
}
