/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

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

}
