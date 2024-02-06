/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es


import sirius.db.mixing.Mixing
import sirius.db.mixing.OptimisticLockException
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

import java.time.Duration

class ElasticSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

            @Part
            private static Mixing mixing

            def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "update / find / delete works"() {
        when:
        ElasticTestEntity entity = new ElasticTestEntity()
        entity.setFirstname("Hello")
        entity.setLastname("World")
        entity.setAge(12)
        elastic.update(entity)
        and:
        elastic.refresh(ElasticTestEntity.class)
                ElasticTestEntity loaded = elastic.findOrFail(ElasticTestEntity.class, entity.getId())
                then:
                loaded.getFirstname() == "Hello"
                loaded.getLastname() == "World"
                loaded.getAge() == 12
                when:
        elastic.delete(entity)
        and:
        elastic.refresh(ElasticTestEntity.class)
                ElasticTestEntity notFound = elastic.find(ElasticTestEntity.class, entity.getId()).orElse(null)
                then:
                notFound == null
    }

    def "update / find / delete works with routing"() {
        when:
        RoutedTestEntity entity = new RoutedTestEntity()
        entity.setFirstname("Hello")
        entity.setLastname("World")
        entity.setAge(12)
        elastic.update(entity)
        and:
        elastic.refresh(RoutedTestEntity.class)
                RoutedTestEntity loaded = elastic.findOrFail(RoutedTestEntity.class, entity.getId(), Elastic.routedBy("World"))
                RoutedTestEntity notLoaded = elastic.find(
                RoutedTestEntity.class,
                        entity.getId(),
                Elastic.routedBy("XX_badRouting")).orElse(null)
                then:
                loaded.getFirstname() == "Hello"
                loaded.getLastname() == "World"
                loaded.getAge() == 12
                and:
                notLoaded == null

                when:
        RoutedTestEntity refreshed = elastic.refreshOrFail(entity)
                then:
                refreshed.getFirstname() == "Hello"

                when:
        elastic.delete(entity)
        and:
        elastic.refresh(RoutedTestEntity.class)
                RoutedTestEntity notFound = elastic.find(RoutedTestEntity.class, entity.getId(), Elastic.routedBy("World")).
        orElse(null)
                then:
                notFound == null
    }

    /**
     * Note that this test only ensures that suppressing the routing works properly.
     * <p>
     * Production code should never mix routed and unrouted access on an entity and expect this to work
     * (Normally this is also rejected and reported by the framework, unless the <tt>elasticsearch.suppressedRoutings</tt>
     * is used).
     */
    def "update / find / delete works with suppressed routing"() {
        when:
        SuppressedRoutedTestEntity entity = new SuppressedRoutedTestEntity()
        entity.setFirstname("Hello")
        entity.setLastname("World")
        entity.setAge(12)
        elastic.update(entity)
        and: "Performing a lookup with routing works"
        elastic.refresh(SuppressedRoutedTestEntity.class)
                SuppressedRoutedTestEntity loaded = elastic.
        findOrFail(SuppressedRoutedTestEntity.class, entity.getId(), Elastic.routedBy("World"))
                and: "Performing a lookup with an invalid routing works"
                SuppressedRoutedTestEntity alsoLoaded = elastic.find(
                SuppressedRoutedTestEntity.class,
                        entity.getId(),
                Elastic.routedBy("XX_badRouting")).orElse(null)
                then:
                loaded.getFirstname() == "Hello"
                loaded.getLastname() == "World"
                loaded.getAge() == 12
                and:
                alsoLoaded.getFirstname() == "Hello"
                alsoLoaded.getLastname() == "World"
                alsoLoaded.getAge() == 12

                and: "A query with a routing works"
                elastic.
                select(SuppressedRoutedTestEntity.class).
        routing("World").
        eq(SuppressedRoutedTestEntity.LASTNAME, "World").
        exists()
                and: "A query without a routing works"
                elastic.
                select(SuppressedRoutedTestEntity.class).
        eq(SuppressedRoutedTestEntity.LASTNAME, "World").
        exists()
                and: "A query with an invalid routing works"
                elastic.
                select(SuppressedRoutedTestEntity.class).
        routing("XXX").
        eq(SuppressedRoutedTestEntity.LASTNAME, "World").
        exists()

                when: "Refresh still works"
        SuppressedRoutedTestEntity refreshed = elastic.refreshOrFail(entity)
        then:
        refreshed.getFirstname() == "Hello"

        when: "Delete works as expected"
        elastic.delete(entity)
        and:
        elastic.refresh(RoutedTestEntity.class)
                SuppressedRoutedTestEntity notFound = elastic.
        find(RoutedTestEntity.class, entity.getId(), Elastic.routedBy("World")).
        orElse(null)
                then:
                notFound == null
    }

    def "optimistic locking works"() {
        when:
        LockedTestEntity entity = new LockedTestEntity()
        entity.setValue("Test")
        elastic.update(entity)
        and:
        elastic.refresh(LockedTestEntity.class)
                LockedTestEntity copyOfOriginal = elastic.refreshOrFail(entity)
                and:
                entity.setValue("Test2")
                elastic.update(entity)
                elastic.refresh(LockedTestEntity.class)
                and:
                entity.setValue("Test3")
                elastic.update(entity)
                elastic.refresh(LockedTestEntity.class)
                and:
                copyOfOriginal.setValue("Test2")
                elastic.tryUpdate(copyOfOriginal)
                then:
                thrown(OptimisticLockException)
                when:
        elastic.tryDelete(copyOfOriginal)
        then:
        thrown(OptimisticLockException)
        when:
        elastic.forceDelete(copyOfOriginal)
        elastic.refresh(LockedTestEntity.class)
                LockedTestEntity notFound = elastic.find(LockedTestEntity.class, entity.getId()).orElse(null)
                then:
                notFound == null
    }

    def "wasCreated() works in elastic"() {
        given:
        ElasticWasCreatedTestEntity e = new ElasticWasCreatedTestEntity()
        e.setValue("test123")
        when:
        elastic.update(e)
        then:
        e.hasJustBeenCreated()
        and:
        elastic.update(e)
        then:
        !e.hasJustBeenCreated()
    }

    def "Deleting a non-existing entity simply does nothing"() {
        given: "An entity when seems to be persisted, but doesn not exist in the database"
        ElasticTestEntity fakeEntity = new ElasticTestEntity()
        fakeEntity.setId("DOES_NOT_EXIST")
        when: "we try to delete it"
        elastic.delete(fakeEntity)
        then: "no exception is thrown as the 404 response by ES is converted into an empty OK response"
        notThrown(HandledException)
    }

    def "Custom write index works"() {
        when: "A new entity is created in the current index"
        ElasticTestEntity testEntity = new ElasticTestEntity()
        testEntity.setFirstname("Test")
        testEntity.setLastname("Entity")
        testEntity.setAge(12)
        elastic.update(testEntity)
        elastic.refresh(ElasticTestEntity.class)
                and: "We switch to a new write index for the entity"
                elastic.createAndInstallWriteIndex(mixing.getDescriptor(ElasticTestEntity.class))
                elastic.refresh(ElasticTestEntity.class)
                and: "Deleting the original entity doesn't change the read index"
                elastic.delete(testEntity)
                elastic.refresh(ElasticTestEntity.class)
                then:
                elastic.find(ElasticTestEntity.class, testEntity.getId()).isPresent()

                when: "Creating another entity is not visible in the read index"
        ElasticTestEntity secondTestEntity = new ElasticTestEntity()
        secondTestEntity.setFirstname("Second")
        secondTestEntity.setLastname("Entity")
        secondTestEntity.setAge(13)
        elastic.update(secondTestEntity)
        elastic.refresh(ElasticTestEntity.class)
                then:
                !elastic.find(ElasticTestEntity.class, secondTestEntity.getId()).isPresent()

                when: "Moving the read alias to the write index..."
        elastic.commitWriteIndex(mixing.getDescriptor(ElasticTestEntity.class))
                then: "...we then see the second entity"
                elastic.find(ElasticTestEntity.class, secondTestEntity.getId()).isPresent()

                when: "...and deleting it is also directly visible"
        elastic.delete(secondTestEntity)
        elastic.refresh(ElasticTestEntity.class)
                then: "The first entity is no longer visible as it has never been written into the write index"
                !elastic.find(ElasticTestEntity.class, testEntity.getId()).isPresent()
    }
}
