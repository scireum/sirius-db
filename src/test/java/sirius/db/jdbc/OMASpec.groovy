/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.db.mixing.OptimisticLockException
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import spock.lang.Stepwise

import java.time.Duration

@Stepwise
class OMASpec extends BaseSpecification {

    @Part
    static OMA oma

    def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "write a test entity and read it back"() {
        given:
        TestEntity e = new TestEntity()
        e.setFirstname("Test")
        e.setLastname("Entity")
        e.setAge(12)
        when:
        oma.update(e)
        then:
        TestEntity readBack = oma.findOrFail(TestEntity.class, e.getId())
        and:
        readBack.getFirstname() == "Test"
        and:
        readBack.getLastname() == "Entity"
        and:
        readBack.getAge() == 12
    }

    def "write and read an entity with composite"() {
        given:
        TestEntityWithComposite e = new TestEntityWithComposite()
        e.getComposite().setCity("x")
        e.getComposite().setStreet("y")
        e.getComposite().setZip("z")
        e.getCompositeWithComposite().getComposite().setCity("a")
        e.getCompositeWithComposite().getComposite().setStreet("b")
        e.getCompositeWithComposite().getComposite().setZip("c")
        when:
        oma.update(e)
        then:
        TestEntityWithComposite readBack = oma.findOrFail(TestEntityWithComposite.class, e.getId())
        and:
        readBack.getComposite().getCity() == "x"
        and:
        readBack.getComposite().getStreet() == "y"
        and:
        readBack.getComposite().getZip() == "z"
        and:
        readBack.getCompositeWithComposite().getComposite().getCity() == "a"
        and:
        readBack.getCompositeWithComposite().getComposite().getStreet() == "b"
        and:
        readBack.getCompositeWithComposite().getComposite().getZip() == "c"
    }

    def "write and read an entity with mixin"() {
        given:
        TestEntityWithMixin e = new TestEntityWithMixin()
        e.setFirstname("Homer")
        e.setLastname("Simpson")
        e.as(TestMixin.class).setMiddleName("Jay")
        e.as(TestMixin.class).as(TestMixinMixin.class).setInitial("J")
        when:
        oma.update(e)
        then:
        TestEntityWithMixin readBack = oma.findOrFail(TestEntityWithMixin.class, e.getId())
        and:
        readBack.getFirstname() == "Homer"
        and:
        readBack.getLastname() == "Simpson"
        and:
        readBack.as(TestMixin.class).getMiddleName() == "Jay"
        and:
        readBack.as(TestMixin.class).as(TestMixinMixin.class).getInitial() == "J"
    }

    def "select from secondary works"() {
        given:
        TestEntity e = new TestEntity()
        e.setFirstname("Marge")
        e.setLastname("Simpson")
        e.setAge(43)
        when:
        oma.update(e)
        and:
        TestEntity readBack = oma.selectFromSecondary(TestEntity.class)
                                 .eq(TestEntity.ID, e.getId())
                                 .queryFirst()
        then:
        readBack != null
        and:
        readBack.getFirstname() == "Marge"
        readBack.getLastname() == "Simpson"
        readBack.getAge() == 43
    }

    def "select not all fields"() {
        given:
        TestEntity e = new TestEntity()
        e.setFirstname("Marge")
        e.setLastname("Simpson")
        e.setAge(43)
        when:
        oma.update(e)
        and:
        TestEntity readBack = oma.select(TestEntity.class)
                                 .eq(TestEntity.ID, e.getId())
                                 .fields(TestEntity.FIRSTNAME, TestEntity.AGE)
                                 .queryFirst()
        then:
        readBack != null
        and:
        !readBack.getDescriptor().isFetched(readBack, readBack.getDescriptor().getProperty(TestEntity.ID))
        readBack.getDescriptor().isFetched(readBack, readBack.getDescriptor().getProperty(TestEntity.FIRSTNAME))
        !readBack.getDescriptor().isFetched(readBack, readBack.getDescriptor().getProperty(TestEntity.LASTNAME))
        readBack.getDescriptor().isFetched(readBack, readBack.getDescriptor().getProperty(TestEntity.AGE))
        and:
        readBack.getFirstname() == "Marge"
        readBack.getLastname() == null
        readBack.getAge() == 43
    }

    def "resolve can resolve an entity by its unique name"() {
        given:
        TestClobEntity test = new TestClobEntity()
        when:
        test.setLargeValue("test")
        and:
        oma.update(test)
        then:
        test == oma.resolveOrFail(test.getUniqueName())
    }

    def "change tracking works"() {
        given:
        TestEntityWithMixin e = new TestEntityWithMixin()
        e.setFirstname("Homer")
        e.setLastname("Simpson")
        e.as(TestMixin.class).setMiddleName("Jay")
        e.as(TestMixin.class).as(TestMixinMixin.class).setInitial("J")
        expect:
        e.isAnyMappingChanged()
        and:
        e.isChanged(TestEntityWithMixin.FIRSTNAME)
        when:
        oma.update(e)
        then:
        !e.isAnyMappingChanged()
        when:
        e.setLastname("SimpsonSimpson")
        then:
        e.isAnyMappingChanged()
        and:
        e.isChanged(TestEntityWithMixin.LASTNAME)
        when:
        oma.update(e)
        and:
        e.as(TestMixin.class).setMiddleName("JayJay")
        then:
        e.isAnyMappingChanged()
        e.isChanged(TestMixin.MIDDLE_NAME.inMixin(TestMixin.class))
    }

    def "optimistic locking works"() {
        when:
        SQLLockedTestEntity entity = new SQLLockedTestEntity()
        entity.setValue("Test")
        oma.update(entity)
        and:
        SQLLockedTestEntity copyOfOriginal = oma.refreshOrFail(entity)
        and:
        entity.setValue("Test2")
        oma.update(entity)
        and:
        entity.setValue("Test3")
        oma.update(entity)
        and:
        copyOfOriginal.setValue("Test2")
        oma.tryUpdate(copyOfOriginal)
        then:
        thrown(OptimisticLockException)
        when:
        oma.tryDelete(copyOfOriginal)
        then:
        thrown(OptimisticLockException)
        when:
        oma.forceDelete(copyOfOriginal)
        SQLLockedTestEntity notFound = oma.find(SQLLockedTestEntity.class, entity.getId()).orElse(null)
        then:
        notFound == null
    }

}
