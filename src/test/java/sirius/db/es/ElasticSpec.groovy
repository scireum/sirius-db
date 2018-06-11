/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import sirius.db.mixing.OptimisticLockException
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part

import java.time.Duration

class ElasticSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

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
        Wait.seconds(2)
        ElasticTestEntity loaded = elastic.findOrFail(ElasticTestEntity.class, entity.getId())
        then:
        loaded.getFirstname() == "Hello"
        loaded.getLastname() == "World"
        loaded.getAge() == 12
        when:
        elastic.delete(entity)
        and:
        Wait.seconds(2)
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
        Wait.seconds(2)
        RoutedTestEntity loaded = elastic.findOrFail(RoutedTestEntity.class, entity.getId(), Elastic.routedBy("World"))
        RoutedTestEntity notLoaded = elastic.find(
                RoutedTestEntity.class,
                entity.getId(),
                Elastic.routedBy("badRouting")).orElse(null)
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
        Wait.seconds(2)
        RoutedTestEntity notFound = elastic.find(RoutedTestEntity.class, entity.getId()).orElse(null)
        then:
        notFound == null
    }

    def "optimistic locking works"() {
        when:
        LockedTestEntity entity = new LockedTestEntity()
        entity.setValue("Test")
        elastic.update(entity)
        and:
        Wait.seconds(2)
        LockedTestEntity copyOfOriginal = elastic.refreshOrFail(entity)
        and:
        entity.setValue("Test2")
        elastic.update(entity)
        Wait.seconds(2)
        and:
        entity.setValue("Test3")
        elastic.update(entity)
        Wait.seconds(2)
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
        Wait.seconds(2)
        LockedTestEntity notFound = elastic.find(LockedTestEntity.class, entity.getId()).orElse(null)
        then:
        notFound == null
    }

}
