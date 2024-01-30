/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mixing.DateRange
import sirius.db.mongo.facets.MongoBooleanFacet
import sirius.db.mongo.facets.MongoDateRangeFacet
import sirius.db.mongo.facets.MongoTermFacet
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.LocalDateTime
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class FacetTest {
    companion object {
        @Part
        private lateinit var mango: Mango
    }

    @Test
    fun `facet search works`() {
        val testEntity1 = MangoTestEntity()
        testEntity1.firstname = "Hello"
        testEntity1.lastname = "World"
        testEntity1.age = 999
        testEntity1.birthday = LocalDateTime.now()
        testEntity1.isCool = true
        testEntity1.superPowers.modify().addAll(listOf("Flying", "X-ray vision"))
        mango.update(testEntity1)

        val testEntity2 = MangoTestEntity()
        testEntity2.firstname = "Hello"
        testEntity2.lastname = "Moto"
        testEntity2.birthday = LocalDateTime.now().minusDays(1)
        testEntity2.age = 999
        testEntity2.isCool = true
        testEntity2.superPowers.modify().addAll(listOf("Flying", "X-ray vision"))
        mango.update(testEntity2)

        val testEntity3 = MangoTestEntity()
        testEntity3.firstname = "Loco"
        testEntity3.lastname = "Moto"
        testEntity3.age = 999
        testEntity3.superPowers.modify().addAll(listOf("Flying", "Time travel"))
        mango.update(testEntity3)

        val firstnameFacet = MongoTermFacet(MangoTestEntity.FIRSTNAME)
        val lastnameFacet = MongoTermFacet(MangoTestEntity.LASTNAME)
        val coolFacet = MongoBooleanFacet(MangoTestEntity.COOL)
        val superPowersFacet = MongoTermFacet(MangoTestEntity.SUPER_POWERS)
        val datesFacet = MongoDateRangeFacet(
                MangoTestEntity.BIRTHDAY,
                listOf(
                        DateRange.TODAY,
                        DateRange.YESTERDAY,
                        DateRange("both",
                                { "both" },
                                {
                                    DateRange.YESTERDAY.from
                                },
                                {
                                    DateRange.TODAY.until
                                }),
                        DateRange.BEFORE_LAST_YEAR
                )
        )
        mango.select(MangoTestEntity::class.java)
                .eq(MangoTestEntity.AGE, 999)
                .addFacet(firstnameFacet)
                .addFacet(lastnameFacet)
                .addFacet(coolFacet)
                .addFacet(datesFacet)
                .addFacet(superPowersFacet)
                .executeFacets()

        assertEquals("Moto", lastnameFacet.values[0].first)
        assertEquals(2, lastnameFacet.values[0].second)
        assertEquals("World", lastnameFacet.values[1].first)
        assertEquals(1, lastnameFacet.values[1].second)

        assertEquals("Hello", firstnameFacet.values[0].first)
        assertEquals(2, firstnameFacet.values[0].second)
        assertEquals("Loco", firstnameFacet.values[1].first)
        assertEquals(1, firstnameFacet.values[1].second)

        assertEquals(2, coolFacet.numTrue)
        assertEquals(1, coolFacet.numFalse)

        assertEquals(1, datesFacet.ranges[0].second)
        assertEquals(1, datesFacet.ranges[1].second)
        assertEquals(2, datesFacet.ranges[2].second)
        assertEquals(0, datesFacet.ranges[3].second)

        assertEquals("Flying", superPowersFacet.values[0].first)
        assertEquals(3, superPowersFacet.values[0].second)
        assertEquals("X-ray vision", superPowersFacet.values[1].first)
        assertEquals(2, superPowersFacet.values[1].second)
        assertEquals("Time travel", superPowersFacet.values[2].first)
        assertEquals(1, superPowersFacet.values[2].second)
    }
}
