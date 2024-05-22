/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.fieldlookup

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.jdbc.OMA
import sirius.db.mixing.FieldLookupCache
import sirius.db.mixing.Mapping
import sirius.db.mongo.Mango
import sirius.kernel.SiriusExtension
import sirius.kernel.cache.Cache
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import sirius.kernel.testutil.Reflections
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class FieldLookupCacheTest {
    @Test
    fun `jdbc field lookup works`() {
        val miles = SQLFieldLookUpTestEntity()
        miles.names.firstname = "Miles"
        miles.names.lastname = "Morales"
        miles.age = 16
        miles.isCool = true
        miles.`as`(SQLSuperHeroTestMixin::class.java).superPowers.modify().addAll(
                listOf(
                        "Agility",
                        "Spider Sense",
                        "Shoot web"
                )
        )
        miles.`as`(SQLSuperHeroTestMixin::class.java).heroNames.firstname = "Spider"
        miles.`as`(SQLSuperHeroTestMixin::class.java).heroNames.lastname = "Man"
        oma.update(miles)

        val cacheKey = lookupCache.getCacheKey(
                (SQLFieldLookUpTestEntity::class.java), miles.id, SQLFieldLookUpTestEntity.NAMES.inner(
                NameFieldsTestComposite.FIRSTNAME
        )
        )
        val name1 = lookupCache.lookup(
                SQLFieldLookUpTestEntity::class.java, miles.id,
                SQLFieldLookUpTestEntity.NAMES.inner(NameFieldsTestComposite.FIRSTNAME)
        )
        val name2 = lookupCache.lookup(
                SQLFieldLookUpTestEntity::class.java, miles.id,
                SQLFieldLookUpTestEntity.NAMES.inner(NameFieldsTestComposite.FIRSTNAME)
        )
        val age = lookupCache.lookup(SQLFieldLookUpTestEntity::class.java, miles.id, SQLFieldLookUpTestEntity.AGE)
        val birthday = lookupCache.lookup(
                SQLFieldLookUpTestEntity::class.java, miles.id,
                SQLFieldLookUpTestEntity.BIRTHDAY
        )
        val cool = lookupCache.lookup(
                SQLFieldLookUpTestEntity::class.java, miles.id,
                SQLFieldLookUpTestEntity.COOL
        )
        val powers = lookupCache.lookup(
                SQLFieldLookUpTestEntity::class.java, miles.id,
                Mapping.mixin(SQLSuperHeroTestMixin::class.java)
                        .inner(SQLSuperHeroTestMixin.SUPER_POWERS)
        )
        val heroFirstName = lookupCache.lookup(
                SQLFieldLookUpTestEntity::class.java, miles.id,
                Mapping.mixin(SQLSuperHeroTestMixin::class.java)
                        .inner(SQLSuperHeroTestMixin.HERO_NAMES)
                        .inner(NameFieldsTestComposite.FIRSTNAME)
        )
        val heroLastName = lookupCache.lookup(
                SQLFieldLookUpTestEntity::class.java, miles.id,
                Mapping.mixin(SQLSuperHeroTestMixin::class.java)
                        .inner(SQLSuperHeroTestMixin.HERO_NAMES)
                        .inner(NameFieldsTestComposite.LASTNAME)
        )

        val cache = Reflections.callPrivateMethod(lookupCache,"getCache") as Cache<String, Value>

        assertEquals("Miles", name1.asString())
        assertEquals("Miles", cache.get(cacheKey).toString())
        assertEquals("Miles", name2.asString())
        assertEquals(16, age.asInt(0))
        assertTrue { cool.asBoolean() }
        assertEquals(listOf("Agility", "Spider Sense", "Shoot web"), powers.get())
        assertEquals("Spider", heroFirstName.asString())
        assertEquals("Man", heroLastName.asString())
    }

    @Test
    fun `mongo field lookup works`() {

        val tony = MongoFieldLookUpTestEntity()
        tony.names.firstname = "Tony"
        tony.names.lastname = "Stark"
        tony.age = 50
        tony.isCool = true
        tony.`as`(MongoSuperHeroTestMixin::class.java).superPowers.modify().addAll(
                listOf(
                        "Flying",
                        "Lasers",
                        "Money"
                )
        )
        tony.`as`(MongoSuperHeroTestMixin::class.java).heroNames.firstname = "Iron"
        tony.`as`(MongoSuperHeroTestMixin::class.java).heroNames.lastname = "Man"
        mango.update(tony)

        val cacheKey = lookupCache.getCacheKey(
                (MongoFieldLookUpTestEntity::class.java), tony.id, MongoFieldLookUpTestEntity.NAMES.inner(
                NameFieldsTestComposite.FIRSTNAME
        )
        )
        val name1 = lookupCache.lookup(
                MongoFieldLookUpTestEntity::class.java, tony.id,
                MongoFieldLookUpTestEntity.NAMES.inner(NameFieldsTestComposite.FIRSTNAME)
        )
        val name2 = lookupCache.lookup(
                MongoFieldLookUpTestEntity::class.java, tony.id,
                MongoFieldLookUpTestEntity.NAMES.inner(NameFieldsTestComposite.FIRSTNAME)
        )
        val age =
                lookupCache.lookup(MongoFieldLookUpTestEntity::class.java, tony.id, MongoFieldLookUpTestEntity.AGE)
        val birthday = lookupCache.lookup(
                MongoFieldLookUpTestEntity::class.java, tony.id,
                MongoFieldLookUpTestEntity.BIRTHDAY
        )
        val cool = lookupCache.lookup(
                MongoFieldLookUpTestEntity::class.java, tony.id,
                MongoFieldLookUpTestEntity.COOL
        )
        val powers = lookupCache.lookup(
                MongoFieldLookUpTestEntity::class.java, tony.id,
                Mapping.mixin(MongoSuperHeroTestMixin::class.java)
                        .inner(MongoSuperHeroTestMixin.SUPER_POWERS)
        )
        val heroFirstName = lookupCache.lookup(
                MongoFieldLookUpTestEntity::class.java, tony.id,
                Mapping.mixin(MongoSuperHeroTestMixin::class.java)
                        .inner(MongoSuperHeroTestMixin.HERO_NAMES)
                        .inner(NameFieldsTestComposite.FIRSTNAME)
        )
        val heroLastName = lookupCache.lookup(
                MongoFieldLookUpTestEntity::class.java, tony.id,
                Mapping.mixin(MongoSuperHeroTestMixin::class.java)
                        .inner(MongoSuperHeroTestMixin.HERO_NAMES)
                        .inner(NameFieldsTestComposite.LASTNAME)
        )

        val cache = Reflections.callPrivateMethod(lookupCache,"getCache") as Cache<String, Value>

        assertEquals("Tony", name1.asString())
        assertEquals("Tony", cache.get(cacheKey).toString())
        assertEquals("Tony", name2.asString())
        assertEquals(50, age.asInt(0))
        assertTrue { cool.asBoolean() }
        assertEquals(listOf("Flying", "Lasers", "Money"), powers.get())
        assertEquals("Iron", heroFirstName.asString())
        assertEquals("Man", heroLastName.asString())
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        @Part
        private lateinit var mango: Mango

        @Part
        private lateinit var lookupCache: FieldLookupCache
    }
}
