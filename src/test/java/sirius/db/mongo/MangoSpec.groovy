/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo


import sirius.db.mixing.IntegrityConstraintFailedException
import sirius.db.mixing.OptimisticLockException
import sirius.kernel.BaseSpecification
import sirius.kernel.Scope
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

class MangoSpec extends BaseSpecification {

    @Part
    private static Mango mango

    @Part
    private static Mongo mongo

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

    def "unique constraint violations are properly thrown"() {
        setup:
        mango.select(MongoUniqueTestEntity.class).eq(MongoUniqueTestEntity.VALUE, "Test").delete()
        when:
        MongoUniqueTestEntity entity = new MongoUniqueTestEntity()
        entity.setValue("Test")
        mango.update(entity)
        and:
        MongoUniqueTestEntity conflictingEntity = new MongoUniqueTestEntity()
        and:
        conflictingEntity.setValue("Test")
        mango.tryUpdate(conflictingEntity)
        then:
        thrown(IntegrityConstraintFailedException)
    }

    @Scope(Scope.SCOPE_NIGHTLY)
    def "selecting over 1000 entities in queryList throws an exception"() {
        given:
        mango.select(MangoListTestEntity.class).delete()
        and:
        for (int i = 0; i < 1001; i++) {
            def entityToCreate = new MangoListTestEntity()
            entityToCreate.setCounter(i)
            mango.update(entityToCreate)
        }
        when:
        mango.select(MangoListTestEntity.class).queryList()
        then:
        thrown(HandledException)
    }

    @Scope(Scope.SCOPE_NIGHTLY)
    def "a timed out mongo count returns an empty optional"() {
        when:
        mango.select(MangoListTestEntity.class).delete()
        and:
        for (int i = 0; i < 100_000; i++) {
            def entityToCreate = new MangoListTestEntity()
            entityToCreate.setCounter(i)
            mango.update(entityToCreate)
        }
        and:
        MongoQuery<MangoListTestEntity> query = mango
                .select(MangoListTestEntity.class)
        then:
        query.count(true, 1) == Optional.empty()
    }

    def "MongoQuery.exists works as expected and leaves the query intact"() {
        when:
        mango.select(MangoListTestEntity.class).delete()
        and:
        for (int i = 0; i < 10; i++) {
            def entityToCreate = new MangoListTestEntity()
            entityToCreate.setCounter(i)
            mango.update(entityToCreate)
        }
        and:
        MongoQuery<MangoListTestEntity> query = mango.
                select(MangoListTestEntity.class).
                orderDesc(MangoListTestEntity.COUNTER)
        then: "simple exists works"
        query.exists() == true
        and: "a count after an exists still yields all entities"
        query.count() == 10
        and: "a list after an exists still yields all entities"
        query.queryList().size() == 10
        and: "a list after an exists still yields all fields"
        query.queryList().get(0).getCounter() == 9
        and: "an exists with a filter also works"
        mango.select(MangoListTestEntity.class).eq(MangoListTestEntity.COUNTER, 5).exists() == true
        and: "an exists with a filter that yields an empty result works"
        mango.select(MangoListTestEntity.class).eq(MangoListTestEntity.COUNTER, 50).exists() == false
    }

    def "MongoQuery.streamBlockwise() works in mango"() {
        when:
        mango.select(MangoListTestEntity.class).delete()
        and:
        for (int i = 0; i < 10; i++) {
            def entityToCreate = new MangoListTestEntity()
            entityToCreate.setCounter(i)
            mango.update(entityToCreate)
        }
        and:
        MongoQuery<MangoListTestEntity> query = mango.select(MangoListTestEntity.class)
        then:
        query.streamBlockwise().count() == 10
        when:
        query.skip(3).limit(0).streamBlockwise().count() == 7
        then:
        thrown(UnsupportedOperationException)
    }

    def "wasCreated() works in mango"() {
        given:
        MangoWasCreatedTestEntity e = new MangoWasCreatedTestEntity()
        e.setValue("test123")
        when:
        mango.update(e)
        then:
        e.hasJustBeenCreated()
        and:
        mango.update(e)
        then:
        !e.hasJustBeenCreated()
    }

    def "a forcefully failed query does not yield any results"() {
        given:
        mango.select(MangoListTestEntity.class).delete()
        and:
        for (int i = 0; i < 3; i++) {
            def entityToCreate = new MangoListTestEntity()
            entityToCreate.setCounter(i)
            mango.update(entityToCreate)
        }
        when:
        def qry = mango.select(MangoListTestEntity.class).fail()
        def flag = false
        then:
        qry.queryList().isEmpty()
        and:
        qry.iterateAll({ e -> flag = true })
        !flag
        and:
        qry.count() == 0
        and:
        !qry.exists()
    }

    def "simple aggregations work"() {
        when:
        def mango1 = new MangoAggregationsTestEntity()
        mango1.setTestInt(30)
        mango.update(mango1)
        def mango2 = new MangoAggregationsTestEntity()
        mango2.setTestInt(10)
        mango.update(mango2)
        def mango3 = new MangoAggregationsTestEntity()
        mango3.setTestInt(20)
        mango.update(mango3)
        then:
        mango.select(MangoAggregationsTestEntity.class).
                aggregateSum(MangoAggregationsTestEntity.TEST_INT).
                asInt(0) == 60
        mango.select(MangoAggregationsTestEntity.class).
                aggregateAverage(MangoAggregationsTestEntity.TEST_INT).
                asDouble(0) == 20.0
        mango.select(MangoAggregationsTestEntity.class).
                aggregateMin(MangoAggregationsTestEntity.TEST_INT).
                asInt(0) == 10
        mango.select(MangoAggregationsTestEntity.class).
                aggregateMax(MangoAggregationsTestEntity.TEST_INT).
                asInt(0) == 30
    }

    def "SkipDefaultValues works as expected"() {
        when:
        SkipDefaultTestEntity test = new SkipDefaultTestEntity()
        mango.update(test)
        and:
        test = mango.find(SkipDefaultTestEntity.class, test.getId()).get()
        then:
        test.getStringTest() == null
        test.isBoolTest() == false
        test.getListTest().size() == 0
        test.getMapTest().size() == 0
        and:
        // Only the id (and _id) is stored...
        mongo.
                find().
                where(SkipDefaultTestEntity.ID, test.getId()).
                singleIn(SkipDefaultTestEntity.class).
                get().
                getUnderlyingObject().
                keySet().
                size() == 2

        when:
        test = new SkipDefaultTestEntity()
        test.setStringTest("Hello")
        test.setBoolTest(true)
        test.getListTest().add("Item")
        test.getMapTest().put("Key", "Value")
        mango.update(test)
        and:
        test = mango.find(SkipDefaultTestEntity.class, test.getId()).get()
        then:
        test.getStringTest() == "Hello"
        test.isBoolTest() == true
        test.getListTest().contains("Item")
        test.getMapTest().get("Key").orElse("") == "Value"
        and:
        // All fields are stored...
        mongo.
                find().
                where(SkipDefaultTestEntity.ID, test.getId()).
                singleIn(SkipDefaultTestEntity.class).
                get().
                getUnderlyingObject().
                keySet().
                size() == 6

        when:
        test.setStringTest(null)
        test.setBoolTest(false)
        test.getListTest().clear()
        test.getMapTest().clear()
        mango.update(test)
        and:
        test = mango.find(SkipDefaultTestEntity.class, test.getId()).get()
        then:
        test.getStringTest() == null
        test.isBoolTest() == false
        test.getListTest().size() == 0
        test.getMapTest().size() == 0
        and:
        // Only the id (and again _id) is stored...
        mongo.
                find().
                where(SkipDefaultTestEntity.ID, test.getId()).
                singleIn(SkipDefaultTestEntity.class).
                get().
                getUnderlyingObject().
                keySet().
                size() == 2

    }
}
