/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.fieldlookup

import sirius.db.jdbc.OMA
import sirius.db.mixing.FieldLookupCache
import sirius.db.mixing.Mapping
import sirius.db.mongo.Mango
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class FieldLookupCacheSpec extends BaseSpecification {

    @Part
    private static OMA oma

            @Part
            private static Mango mango

            @Part
            private static FieldLookupCache lookupCache

            def "jdbc field lookup works"() {
        given:
        SQLFieldLookUpTestEntity miles = new SQLFieldLookUpTestEntity()
        miles.getNames().setFirstname("Miles")
        miles.getNames().setLastname("Morales")
        miles.setAge(16)
        miles.setCool(true)
        miles.as(SQLSuperHeroTestMixin.class).getSuperPowers().modify().addAll(Arrays.asList("Agility",
                "Spider Sense",
                "Shoot web"))
                miles.as(SQLSuperHeroTestMixin.class).getHeroNames().setFirstname("Spider")
        miles.as(SQLSuperHeroTestMixin.class).getHeroNames().setLastname("Man")
                oma.update(miles)
                when:
        def cacheKey = lookupCache.
        getCacheKey((SQLFieldLookUpTestEntity.class), miles.getId(), SQLFieldLookUpTestEntity.NAMES.inner(
                NameFieldsTestComposite.FIRSTNAME))
                def name1 = lookupCache.lookup(SQLFieldLookUpTestEntity.class, miles.getId(),
                SQLFieldLookUpTestEntity.NAMES.inner(NameFieldsTestComposite.FIRSTNAME))
                def name2 = lookupCache.lookup(SQLFieldLookUpTestEntity.class, miles.getId(),
                SQLFieldLookUpTestEntity.NAMES.inner(NameFieldsTestComposite.FIRSTNAME))
                def age = lookupCache.lookup(SQLFieldLookUpTestEntity.class, miles.getId(), SQLFieldLookUpTestEntity.AGE)
                def birthday = lookupCache.lookup(SQLFieldLookUpTestEntity.class, miles.getId(),
                SQLFieldLookUpTestEntity.BIRTHDAY)
                def cool = lookupCache.lookup(SQLFieldLookUpTestEntity.class, miles.getId(),
                SQLFieldLookUpTestEntity.COOL)
                def powers = lookupCache.lookup(SQLFieldLookUpTestEntity.class, miles.getId(),
                Mapping.mixin(SQLSuperHeroTestMixin.class)
                        .inner(SQLSuperHeroTestMixin.SUPER_POWERS))
                        def heroFirstName = lookupCache.lookup(SQLFieldLookUpTestEntity.class, miles.getId(),
                        Mapping.mixin(SQLSuperHeroTestMixin.class)
                                .inner(SQLSuperHeroTestMixin.HERO_NAMES)
                                .inner(NameFieldsTestComposite.FIRSTNAME))
                                def heroLastName = lookupCache.lookup(SQLFieldLookUpTestEntity.class, miles.getId(),
                                Mapping.mixin(SQLSuperHeroTestMixin.class)
                                        .inner(SQLSuperHeroTestMixin.HERO_NAMES)
                                        .inner(NameFieldsTestComposite.LASTNAME))

                                        then:
                                        name1.asString() == "Miles"
                                        lookupCache.cache.get(cacheKey).asString() == "Miles"
                                        name2.asString() == "Miles"
                                        age.asInt(0) == 16
                                !!cool == true
                                powers.get() == Arrays.asList("Agility", "Spider Sense", "Shoot web")
                                heroFirstName.asString() == "Spider"
                                heroLastName.asString() == "Man"
    }

    def "mongo field lookup works"() {
        given:
        MongoFieldLookUpTestEntity tony = new MongoFieldLookUpTestEntity()
        tony.getNames().setFirstname("Tony")
        tony.getNames().setLastname("Stark")
        tony.setAge(50)
        tony.setCool(true)
        tony.as(MongoSuperHeroTestMixin.class).getSuperPowers().modify().addAll(Arrays.asList("Flying",
                "Lasers",
                "Money"))
                tony.as(MongoSuperHeroTestMixin.class).getHeroNames().setFirstname("Iron")
        tony.as(MongoSuperHeroTestMixin.class).getHeroNames().setLastname("Man")
                mango.update(tony)
                when:
        def cacheKey = lookupCache.
        getCacheKey((MongoFieldLookUpTestEntity.class), tony.getId(), MongoFieldLookUpTestEntity.NAMES.inner(
                NameFieldsTestComposite.FIRSTNAME))
                def name1 = lookupCache.lookup(MongoFieldLookUpTestEntity.class, tony.getId(),
                MongoFieldLookUpTestEntity.NAMES.inner(NameFieldsTestComposite.FIRSTNAME))
                def name2 = lookupCache.lookup(MongoFieldLookUpTestEntity.class, tony.getId(),
                MongoFieldLookUpTestEntity.NAMES.inner(NameFieldsTestComposite.FIRSTNAME))
                def age = lookupCache.lookup(MongoFieldLookUpTestEntity.class, tony.getId(), MongoFieldLookUpTestEntity.AGE)
                def birthday = lookupCache.lookup(MongoFieldLookUpTestEntity.class, tony.getId(),
                MongoFieldLookUpTestEntity.BIRTHDAY)
                def cool = lookupCache.lookup(MongoFieldLookUpTestEntity.class, tony.getId(),
                MongoFieldLookUpTestEntity.COOL)
                def powers = lookupCache.lookup(MongoFieldLookUpTestEntity.class, tony.getId(),
                Mapping.mixin(MongoSuperHeroTestMixin.class)
                        .inner(MongoSuperHeroTestMixin.SUPER_POWERS))
                        def heroFirstName = lookupCache.lookup(MongoFieldLookUpTestEntity.class, tony.getId(),
                        Mapping.mixin(MongoSuperHeroTestMixin.class)
                                .inner(MongoSuperHeroTestMixin.HERO_NAMES)
                                .inner(NameFieldsTestComposite.FIRSTNAME))
                                def heroLastName = lookupCache.lookup(MongoFieldLookUpTestEntity.class, tony.getId(),
                                Mapping.mixin(MongoSuperHeroTestMixin.class)
                                        .inner(MongoSuperHeroTestMixin.HERO_NAMES)
                                        .inner(NameFieldsTestComposite.LASTNAME))
                                        then:
                                        name1.asString() == "Tony"
                                        lookupCache.cache.get(cacheKey).asString() == "Tony"
                                        name2.asString() == "Tony"
                                        age.asInt(0) == 50
                                !!cool == true
                                powers.get() == Arrays.asList("Flying", "Lasers", "Money")
                                heroFirstName.asString() == "Iron"
                                heroLastName.asString() == "Man"
    }
}
