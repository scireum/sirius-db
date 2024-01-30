/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mixing.Mapping
import sirius.db.mongo.properties.MongoStringListEntity
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import java.util.*
import kotlin.jvm.optionals.getOrNull
import kotlin.test.*

@ExtendWith(SiriusExtension::class)
class MongoFilterFactoryTest {
    @Test
    fun `prefix search works`() {
        val prefixTestEntity = PrefixTestEntity()
        prefixTestEntity.prefix = "test-1"
        mango.update(prefixTestEntity)

        assertNotNull(prefixSearch("te"))
        assertNotNull(prefixSearch("test-"))
        assertNotNull(prefixSearch("Test-1"))
        assertNotNull(textSearch("Test-1"))
        assertNotNull(textSearch("Test"))
        assertNotNull(textSearch("test-1"))
        assertNull(textSearch("te"))
    }

    @Test
    fun `prefix with leading number works`() {
        val prefixTestEntity = PrefixTestEntity()
        prefixTestEntity.prefix = "1-test"
        mango.update(prefixTestEntity)

        assertNotNull(prefixSearch("1"))
        assertNotNull(prefixSearch("1-t"))
        assertNotNull(prefixSearch("1-test"))
        assertNotNull(prefixSearch("1-TEST"))
    }

    @Test
    fun `oneInField query works`() {
        val mongoStringListEntity = MongoStringListEntity()
        mongoStringListEntity.list.modify().addAll(listOf("1", "2", "3"))
        val entityEmpty = MongoStringListEntity()

        mango.update(mongoStringListEntity)
        mango.update(entityEmpty)

        assertEquals(
                mongoStringListEntity.id, mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, listOf("2", "4", "5")).build())
                .queryOne().getId()
        )
        assertEquals(
                mongoStringListEntity.id, mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, listOf("2", "3", "4")).build())
                .queryOne().getId()
        )
        assertEquals(
                0, mango.select(
                MongoStringListEntity::class.java
        )
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, listOf("4", "5", "6")).build())
                .count()
        )
        assertEquals(
                entityEmpty.id, mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(
                        QueryBuilder.FILTERS.oneInField(MongoStringListEntity.LIST, listOf("4", "5", "6")).orEmpty()
                                .build()
                )
                .queryOne().getId()
        )
    }

    @Test
    fun `containsAny query works`() {
        val mongoStringListEntity = MongoStringListEntity()
        mongoStringListEntity.list.modify().addAll(listOf("1", "2", "3"))
        val entityEmpty = MongoStringListEntity()

        mango.update(mongoStringListEntity)
        mango.update(entityEmpty)

        assertEquals(
                mongoStringListEntity.getId(), mango.select(
                MongoStringListEntity::class.java
        )
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST, Value.of("2,4,5")).build())
                .queryOne().getId()
        )
        assertEquals(
                mongoStringListEntity.getId(), mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST, Value.of("2,3,4")).build())
                .queryOne().getId()
        )
        assertEquals(
                0, mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST, Value.of("4,5,6")).build())
                .count()
        )
        assertEquals(
                entityEmpty.id, mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(
                        QueryBuilder.FILTERS.containsAny(MongoStringListEntity.LIST, Value.of("4,5,6"))
                                .orEmpty().build()
                )
                .queryOne().getId()
        )
    }

    @Test
    fun `complex OR-constraint can be inverted`() {
        val mongoStringListEntity = MongoStringListEntity()
        mongoStringListEntity.list.modify().addAll(listOf("1", "2", "3", "4"))
        val entityEmpty = MongoStringListEntity()
        val fakeField = Mapping.named("fakeField")

        mango.update(mongoStringListEntity)
        mango.update(entityEmpty)

        assertEquals(
                0, mango.select(
                MongoStringListEntity::class.java
        )
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(
                        QueryBuilder.FILTERS.not(
                                QueryBuilder.FILTERS.containsAny(
                                        MongoStringListEntity.LIST,
                                        Value.of("4,5,6")
                                ).build()
                        )
                )
                .count()
        )
        assertEquals(
                entityEmpty.getId(), mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(
                        QueryBuilder.FILTERS.not(
                                QueryBuilder.FILTERS.containsAny(
                                        MongoStringListEntity.LIST,
                                        Value.of("4,5,6")
                                ).build()
                        )
                )
                .queryOne().getId()
        )
        assertEquals(
                0, mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(
                        QueryBuilder.FILTERS.not(
                                QueryBuilder.FILTERS.containsAny(
                                        MongoStringListEntity.LIST,
                                        Value.of("4,5,6")
                                ).orEmpty().build()
                        )
                )
                .count()
        )
        assertEquals(
                entityEmpty.getId(), mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(
                        QueryBuilder.FILTERS.not(
                                QueryBuilder.FILTERS.or(
                                        QueryBuilder.FILTERS.eq(fakeField, "someValue"),
                                        QueryBuilder.FILTERS.eq(fakeField, "otherValue")
                                )
                        )
                )
                .queryOne().getId()
        )
        assertEquals(
                0, mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, entityEmpty.getId())
                .where(
                        QueryBuilder.FILTERS.not(
                                QueryBuilder.FILTERS.or(
                                        QueryBuilder.FILTERS.eq(fakeField, "someValue"),
                                        QueryBuilder.FILTERS.eq(fakeField, "otherValue"),
                                        QueryBuilder.FILTERS.notExists(fakeField)
                                )
                        )
                )
                .count()
        )
    }

    @Test
    fun `complex AND-constraint cannot be inverted`() {
        val mongoStringListEntity = MongoStringListEntity()
        mongoStringListEntity.list.modify().addAll(listOf("1", "2", "3"))
        val entityEmpty = MongoStringListEntity()
        mango.update(mongoStringListEntity)
        mango.update(entityEmpty)

        assertThrows<IllegalArgumentException> {
            mango.select(
                    MongoStringListEntity::class.java
            )
                    .eq(MongoEntity.ID, entityEmpty.getId())
                    .where(
                            QueryBuilder.FILTERS.not(
                                    QueryBuilder.FILTERS.containsAll(
                                            MongoStringListEntity.LIST,
                                            Value.of("1,2,3,4")
                                    ).build()
                            )
                    )
                    .queryOne()
        }
    }

    @Test
    fun `noneInField query works`() {
        val mongoStringListEntity = MongoStringListEntity()
        mongoStringListEntity.list.modify().addAll(listOf("1", "2", "3"))

        mango.update(mongoStringListEntity)

        assertEquals(
                0, mango.select(
                MongoStringListEntity::class.java
        )
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.noneInField(MongoStringListEntity.LIST, listOf("2")))
                .count()
        )
        assertEquals(
                mongoStringListEntity.id, mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.noneInField(MongoStringListEntity.LIST, listOf("5")))
                .queryOne().getId()
        )
    }

    @Test
    fun `allInField query works`() {
        val mongoStringListEntity = MongoStringListEntity()
        mongoStringListEntity.list.modify().addAll(listOf("1", "2", "3"))

        mango.update(mongoStringListEntity)

        assertEquals(
                0, mango.select(
                MongoStringListEntity::class.java
        )
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.allInField(MongoStringListEntity.LIST, listOf("1", "2", "3", "4")))
                .count()
        )
        assertEquals(
                mongoStringListEntity.getId(), mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.allInField(MongoStringListEntity.LIST, listOf("1", "2", "3")))
                .queryOne().getId()
        )
        assertEquals(
                mongoStringListEntity.getId(), mango.select(MongoStringListEntity::class.java)
                .eq(MongoEntity.ID, mongoStringListEntity.getId())
                .where(QueryBuilder.FILTERS.allInField(MongoStringListEntity.LIST, listOf("1", "2")))
                .queryOne().getId()
        )
    }

    @Test
    fun `automatic 'and' works for fields`() {
        mango.select(
                MangoTestEntity::class.java
        ).delete()

        val mangoTestEntity = MangoTestEntity()
        mangoTestEntity.firstname = "AND"
        mangoTestEntity.lastname = "WORKS"
        mango.update(mangoTestEntity)

        assertEquals(
                1, mongo.find()
                .where(MangoTestEntity.LASTNAME, "WORKS")
                .countIn(
                        MangoTestEntity::class.java
                )
        )
        assertEquals(
                0, mongo.find()
                .where(MangoTestEntity.LASTNAME, "WORKS")
                .where(MangoTestEntity.LASTNAME, "FAILS")
                .where(MangoTestEntity.LASTNAME, "FAILS-YET-AGAIN")
                .where(MangoTestEntity.LASTNAME, "FAILS-THE-LAST-TIME")
                .countIn(
                        MangoTestEntity::class.java
                )
        )

        assertEquals(
                1, mongo.find().where(MangoTestEntity.LASTNAME, "WORKS")
                .where(MangoTestEntity.FIRSTNAME, "AND")
                .countIn(
                        MangoTestEntity::class.java
                )
        )

        assertEquals(
                0, mongo.find()
                .where(
                        QueryBuilder.FILTERS.and(
                                QueryBuilder.FILTERS.eq(
                                        MangoTestEntity.LASTNAME,
                                        "WORKS"
                                ),
                                QueryBuilder.FILTERS.eq(
                                        MangoTestEntity.FIRSTNAME,
                                        "AND"
                                )
                        )
                )
                .where(
                        QueryBuilder.FILTERS.and(
                                QueryBuilder.FILTERS.eq(
                                        MangoTestEntity.LASTNAME,
                                        "FAILS"
                                ),
                                QueryBuilder.FILTERS.eq(
                                        MangoTestEntity.FIRSTNAME,
                                        "AND"
                                )
                        )
                )
                .countIn(
                        MangoTestEntity::class.java
                )
        )

        assertEquals(
                1, mongo.find()
                .where(
                        QueryBuilder.FILTERS.and(
                                QueryBuilder.FILTERS.eq(
                                        MangoTestEntity.LASTNAME,
                                        "WORKS"
                                ),
                                QueryBuilder.FILTERS.eq(
                                        MangoTestEntity.FIRSTNAME,
                                        "AND"
                                )
                        )
                )
                .where(
                        QueryBuilder.FILTERS.and(
                                QueryBuilder.FILTERS.eq(
                                        MangoTestEntity.LASTNAME,
                                        "WORKS"
                                ),
                                QueryBuilder.FILTERS.eq(
                                        MangoTestEntity.FIRSTNAME,
                                        "AND"
                                )
                        )
                )
                .countIn(MangoTestEntity::class.java)
        )
    }

    @Test
    fun `automatic 'and' works for multiple ands`() {

        val mangoTestEntity = MangoTestEntity()
        mangoTestEntity.firstname = "AND1"
        mangoTestEntity.lastname = "WORKS1"
        mango.update(mangoTestEntity)

        assertEquals(
                0, mongo.find()
                .where(
                        QueryBuilder.FILTERS.and(
                                QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS1"),
                                QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND1")
                        )
                )
                .where(
                        QueryBuilder.FILTERS.and(
                                QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "FAILS"),
                                QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND1")
                        )
                )
                .countIn(
                        MangoTestEntity::class.java
                )
        )

        assertEquals(
                1, mongo.find()
                .where(
                        QueryBuilder.FILTERS.and(
                                QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS1"),
                                QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND1")
                        )
                )
                .where(
                        QueryBuilder.FILTERS.and(
                                QueryBuilder.FILTERS.eq(MangoTestEntity.LASTNAME, "WORKS1"),
                                QueryBuilder.FILTERS.eq(MangoTestEntity.FIRSTNAME, "AND1")
                        )
                )
                .countIn(MangoTestEntity::class.java)
        )
    }

    @Test
    fun `isEmptyList works on List fields`() {
        mango.select(
                MangoTestEntity::class.java
        ).delete()

        val mangoTestEntity1 = MangoTestEntity()
        mangoTestEntity1.firstname = "Peter"
        mangoTestEntity1.lastname = "Parker"
        mango.update(mangoTestEntity1)
        val mangoTestEntity2 = MangoTestEntity()
        mangoTestEntity2.firstname = "Spider"
        mangoTestEntity2.lastname = "Man"
        mangoTestEntity2.superPowers.add("Wallcrawling")
        mango.update(mangoTestEntity2)

        assertEquals(
                1, mango.select(MangoTestEntity::class.java)
                .where(QueryBuilder.FILTERS.isEmptyList(MangoTestEntity.SUPER_POWERS)).count()
        )
    }

    @Test
    fun `forceEmpty works on List fields`() {
        mango.select(
                MangoTestEntity::class.java
        ).delete()

        val mangoTestEntity1 = MangoTestEntity()
        mangoTestEntity1.firstname = "Peter"
        mangoTestEntity1.lastname = "Parker"
        mango.update(mangoTestEntity1)

        val mangoTestEntity2 = MangoTestEntity()
        mangoTestEntity2.firstname = "Spider"
        mangoTestEntity2.lastname = "Man"
        mangoTestEntity2.superPowers.add("Wallcrawling")
        mango.update(mangoTestEntity2)

        assertEquals(
                1, mango.select(MangoTestEntity::class.java)
                .where(
                        QueryBuilder.FILTERS.oneInField(MangoTestEntity.SUPER_POWERS, Collections.EMPTY_LIST)
                                .forceEmpty().build()
                ).count()
        )
    }

    @Test
    fun `hasListSize works on List fields`() {
        mango.select(
                MangoTestEntity::class.java
        ).delete()

        val mangoTestEntity1 = MangoTestEntity()
        mangoTestEntity1.firstname = "Peter"
        mangoTestEntity1.lastname = "Parker"
        mangoTestEntity1.superPowers.add("Häkeln")
        mangoTestEntity1.superPowers.add("Stricken")
        mangoTestEntity1.superPowers.add("Klöppeln")
        mango.update(mangoTestEntity1)

        val mangoTestEntity2 = MangoTestEntity()
        mangoTestEntity2.firstname = "Spider"
        mangoTestEntity2.lastname = "Man"
        mangoTestEntity2.superPowers.add("Wallcrawling")
        mangoTestEntity2.superPowers.add("Unkrautjäten")
        mangoTestEntity2.superPowers.add("Rasenmähen")
        mangoTestEntity2.superPowers.add("Fensterputzen")
        mango.update(mangoTestEntity2)

        assertEquals(
                0, mango.select(
                MangoTestEntity::class.java
        )
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 0)).count()
        )

        assertEquals(
                0, mango.select(MangoTestEntity::class.java)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 1)).count()
        )
        assertEquals(
                0, mango.select(MangoTestEntity::class.java)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 2)).count()
        )
        assertEquals(
                1, mango.select(MangoTestEntity::class.java)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 3)).count()
        )
        assertEquals(
                1, mango.select(MangoTestEntity::class.java)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 4)).count()
        )
        assertEquals(
                0, mango.select(MangoTestEntity::class.java)
                .where(QueryBuilder.FILTERS.hasListSize(MangoTestEntity.SUPER_POWERS, 5)).count()
        )
    }

    companion object {
        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var mongo: Mongo

        private fun prefixSearch(query: String): Doc? {
            return mongo.find().where(QueryBuilder.FILTERS.prefix(PrefixTestEntity.PREFIX, query))
                    .singleIn(PrefixTestEntity::class.java).getOrNull()
        }

        private fun textSearch(query: String): Doc? {
            return mongo.find().where(QueryBuilder.FILTERS.text(query))
                    .singleIn(PrefixTestEntity::class.java).getOrNull()
        }
    }
}
