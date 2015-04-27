/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.db.jdbc.SQLQuery
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.mixing.OMA
import sirius.mixing.SmartQuery
import sirius.mixing.constraints.FieldEqual
import spock.lang.Stepwise

@Stepwise
class OMASpec extends BaseSpecification {

    @Part
    static OMA oma;

    def "write a test entity and read it back"() {
        given:
        def e = new TestEntity();
        e.setFirstname("Test");
        e.setLastname("Entity");
        e.setAge(12);
        when:
        oma.update(e);
        then:
        TestEntity readBack = oma.findOrFail(TestEntity.class, e.getId());
        and:
        readBack.getFirstname() == "Test"
        and:
        readBack.getLastname() == "Entity"
        and:
        readBack.getAge() == 12
    }

    def "write and read an entity with composite"() {
        given:
        def e = new TestEntityWithComposite();
        e.getComposite().setCity("x")
        e.getComposite().setStreet("y")
        e.getComposite().setZip("z")
        e.getCompositeWithComposite().getComposite().setCity("a")
        e.getCompositeWithComposite().getComposite().setStreet("b")
        e.getCompositeWithComposite().getComposite().setZip("c")
        when:
        oma.update(e);
        then:
        TestEntityWithComposite readBack = oma.findOrFail(TestEntityWithComposite.class, e.getId());
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
        def e = new TestEntityWithMixin();
        e.setFirstname("Homer");
        e.setLastname("Simpson");
        e.as(TestMixin.class).setMiddleName("Jay");
        e.as(TestMixin.class).as(TestMixinMixin.class).setInitial("J")
        when:
        oma.update(e);
        then:
        TestEntityWithMixin readBack = oma.findOrFail(TestEntityWithMixin.class, e.getId());
        and:
        readBack.getFirstname() == "Homer"
        and:
        readBack.getLastname() == "Simpson"
        and:
        readBack.as(TestMixin.class).getMiddleName() == "Jay"
        and:
        readBack.as(TestMixin.class).as(TestMixinMixin.class).getInitial() == "J"
    }

    def "transform works when reading a TestEntity"() {
        given:
        SQLQuery qry = oma.getDatabase().createQuery("SELECT * FROM testentity");
        when:
        def e = oma.transform(TestEntity.class, qry).queryFirst();
        then:
        e.getFirstname() == "Test"
        and:
        e.getLastname() == "Entity"
        and:
        e.getAge() == 12
    }

    def "transform works when reading a TestEntity with alias"() {
        given:
        SQLQuery qry = oma.getDatabase().createQuery("SELECT id as x_id, lastname as x_lastname  FROM testentity");
        when:
        def e = oma.transform(TestEntity.class, "x", qry).queryFirst();
        then:
        e.getLastname() == "Entity"
    }

    def "transform works when reading a TestEntity with a computed column"() {
        given:
        SQLQuery qry = oma.getDatabase().createQuery("SELECT id, lastname, age * 2 AS doubleAge FROM testentity");
        when:
        def e = oma.transform(TestEntity.class, qry).queryFirst();
        then:
        e.getLastname() == "Entity"
        and:
        e.getFetchRow().getValue("doubleAge").asInt(-1) == 24
    }

    def "simple query works"() {
        given:
        SmartQuery<TestEntity> qry = oma.select(TestEntity.class).where(new FieldEqual("firstname","Test")).orderAsc("lastname");
        when:
        def e = qry.queryList();
        then:
        e.size() == 1
    }

}
