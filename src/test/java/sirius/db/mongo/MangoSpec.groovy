/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo


import sirius.db.mixing.OptimisticLockException
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

class MangoSpec extends BaseSpecification {

    @Part
    private static Mango mango

    def "write a test entity and read it back"() {
        given:
        MangoTestEntity e = new MangoTestEntity()
        e.setFirstname("Test")
        e.setLastname("Entity")
        e.setAge(12)
        when:
        mango.update(e)
        then:
        MangoTestEntity readBack = mango.findOrFail(MangoTestEntity.class, e.getId())
        and:
        readBack.getFirstname() == "Test"
        and:
        readBack.getLastname() == "Entity"
        and:
        readBack.getAge() == 12
    }

    def "select not all fields"() {
        given:
        MangoTestEntity e = new MangoTestEntity()
        e.setFirstname("Test2")
        e.setLastname("Entity2")
        e.setAge(13)
        when:
        mango.update(e)
        and:
        MangoTestEntity readBack = mango.select(MangoTestEntity.class)
                                        .eq(MangoTestEntity.ID, e.getId())
                                        .fields(MangoTestEntity.FIRSTNAME, MangoTestEntity.AGE)
                                        .queryFirst()
        then:
        readBack != null
        and:
        !readBack.getDescriptor().isFetched(readBack, readBack.getDescriptor().getProperty(MangoTestEntity.ID))
        readBack.getDescriptor().isFetched(readBack, readBack.getDescriptor().getProperty(MangoTestEntity.FIRSTNAME))
        !readBack.getDescriptor().isFetched(readBack, readBack.getDescriptor().getProperty(MangoTestEntity.LASTNAME))
        readBack.getDescriptor().isFetched(readBack, readBack.getDescriptor().getProperty(MangoTestEntity.AGE))
        and:
        readBack.getFirstname() == "Test2"
        readBack.getLastname() == null
        readBack.getAge() == 13
    }

    def "delete an entity"() {
        given:
        MangoTestEntity e = new MangoTestEntity()
        e.setFirstname("Test")
        e.setLastname("Entity")
        e.setAge(12)
        when:
        mango.update(e)
        and:
        def refreshed = mango.tryRefresh(e)
        and:
        mango.delete(e)
        and:
        mango.refreshOrFail(e)
        then:
        // The first refresh worked
        refreshed == e
        and:
        // But did not return the original entity
        // but a fresh instance from the DB
        !refreshed.is(e)
        and:
        // The second refresh failed as expected
        thrown(HandledException)
    }

    def "optimistic locking works"() {
        when:
        MongoLockedTestEntity entity = new MongoLockedTestEntity()
        entity.setValue("Test")
        mango.update(entity)
        and:
        MongoLockedTestEntity copyOfOriginal = mango.refreshOrFail(entity)
        and:
        entity.setValue("Test2")
        mango.update(entity)
        and:
        entity.setValue("Test3")
        mango.update(entity)
        and:
        copyOfOriginal.setValue("Test2")
        mango.tryUpdate(copyOfOriginal)
        then:
        thrown(OptimisticLockException)
        when:
        mango.tryDelete(copyOfOriginal)
        then:
        thrown(OptimisticLockException)
        when:
        mango.forceDelete(copyOfOriginal)
        MongoLockedTestEntity notFound = mango.find(MongoLockedTestEntity.class, entity.getId()).orElse(null)
        then:
        notFound == null
    }

    def "selecting over 1000 entities in queryList throws an exception"() {
        given:
        mango.select(ListTestEntity.class).delete()
        and:
        for (int i = 0; i < 1001; i++) {
            def entityToCreate = new ListTestEntity()
            entityToCreate.setCounter(i)
            mango.update(entityToCreate)
        }
        when:
        mango.select(ListTestEntity.class).queryList()
        then:
        thrown(HandledException)
    }

}
