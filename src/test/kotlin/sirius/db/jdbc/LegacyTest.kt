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
import sirius.db.mixing.Mapping
import sirius.db.mixing.Mixing
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

@ExtendWith(SiriusExtension::class)
class LegacyTest {
    @Test
    fun `check if aliasing for columns work`() {
        val legacyEntity = LegacyEntity()
        legacyEntity.firstname = "Test"
        legacyEntity.lastname = "Entity"
        legacyEntity.composite.street = "Street"
        legacyEntity.composite.city = "Test-City"
        legacyEntity.composite.zip = "1245"
        oma.update(legacyEntity)

        val fromDB = oma.select(LegacyEntity::class.java)
                .eq(Mapping.named("firstname"), "Test")
                .orderAsc(Mapping.named("composite").inner(Mapping.named("street")))
                .queryFirst()

        assertFalse { legacyEntity.isNew }
        assertNotNull(fromDB)
        assertEquals("Test", fromDB.firstname)
        assertEquals("Street", fromDB.composite.street)
        assertNotNull(
                oma.getDatabase(Mixing.DEFAULT_REALM)
                        ?.createQuery("SELECT * FROM banana WHERE name1 = 'Test' and street = 'Street'")?.first()
        )
    }

    @Test
    fun `updating and change tracking on an entity with aliased columns works`() {
        var legacyEntity = LegacyEntity()
        legacyEntity.firstname = "Test2"
        legacyEntity.lastname = "Entity2"
        legacyEntity.composite.street = "Streeet2"
        legacyEntity.composite.city = "Test-City2"
        legacyEntity.composite.zip = "12452"
        oma.update(legacyEntity)
        legacyEntity.composite.street = "Street3"
        oma.update(legacyEntity)
        legacyEntity = oma.refreshOrFail(legacyEntity)

        assertEquals("Street3", legacyEntity.composite.street)
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        fun setupSpec() {
            oma.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
