/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.db.jdbc.schema.Schema
import sirius.db.mixing.Mixing
import sirius.kernel.BaseSpecification
import sirius.kernel.Scope
import sirius.kernel.commons.Strings
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import spock.lang.Stepwise

import java.util.function.Function
import java.util.stream.Collectors

@Stepwise
class SmartQuerySpec extends BaseSpecification {

    @Part
    static OMA oma

    def setupSpec() {
        oma.select(SmartQueryTestEntity.class).delete()
        oma.select(SmartQueryTestParentEntity.class).delete()
        oma.select(SmartQueryTestChildEntity.class).delete()
        oma.select(SmartQueryTestChildChildEntity.class).delete()

        fillSmartQueryTestEntity()
        fillSmartQueryTestChildAndParentEntity()
    }

    private void fillSmartQueryTestEntity() {
        SmartQueryTestEntity e = new SmartQueryTestEntity()
        e.setValue("Test")
        e.setTestNumber(1)
        oma.update(e)
        e = new SmartQueryTestEntity()
        e.setValue("Hello")
        e.setTestNumber(2)
        oma.update(e)
        e = new SmartQueryTestEntity()
        e.setValue("World")
        e.setTestNumber(3)
        oma.update(e)
    }

    private void fillSmartQueryTestChildAndParentEntity() {
        SmartQueryTestParentEntity p1 = new SmartQueryTestParentEntity()
        p1.setName("Parent 1")
        oma.update(p1)
        SmartQueryTestParentEntity p2 = new SmartQueryTestParentEntity()
        p2.setName("Parent 2")
        oma.update(p2)


        SmartQueryTestChildEntity c = new SmartQueryTestChildEntity()
        c.setName("Child 1")
        c.getParent().setId(p1.getId())
        c.getOtherParent().setId(p2.getId())
        oma.update(c)

        SmartQueryTestChildChildEntity cc = new SmartQueryTestChildChildEntity()
        cc.setName("ChildChild 1")
        cc.getParentChild().setValue(c)
        oma.update(cc)

        c = new SmartQueryTestChildEntity()
        c.setName("Child 2")
        c.getParent().setId(p2.getId())
        c.getOtherParent().setId(p1.getId())
        oma.update(c)

    }

    def "queryList returns all entities"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        when:
        def result = qry.queryList()
        then:
        result.stream().
                map({ x -> x.getValue() } as Function).
                collect(Collectors.toList()) == ["Test", "Hello", "World"]
    }

    def "count returns the number of entity"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        when:
        def result = qry.count()
        then:
        result == 3
    }

    def "exists returns a correct value"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        when:
        def result = qry.exists()
        then:
        result == true
    }

    def "queryFirst returns the first entity"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        when:
        def result = qry.queryFirst()
        then:
        result.getValue() == "Test"
    }

    def "first returns the first entity"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        when:
        def result = qry.first()
        then:
        result.get().getValue() == "Test"
    }

    def "queryFirst returns null for an empty result"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .eq(SmartQueryTestEntity.VALUE, "xxx")
        when:
        def result = qry.queryFirst()
        then:
        result == null
    }

    def "first returns an empty optional for an empty result"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .eq(SmartQueryTestEntity.VALUE, "xxx")
        when:
        def result = qry.first()
        then:
        !result.isPresent()
    }

    def "limit works on a plain list"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .skip(1).limit(2).orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        when:
        def result = qry.queryList()
        then:
        result.stream().map({ x -> x.getValue() } as Function).collect(Collectors.toList()) == ["Hello", "World"]
    }

    def "limit works on a plain list (skipping native LIMIT)"() {
        given:
        def noCapsDB = Mock(Database)
        noCapsDB.hasCapability(_) >> false
        noCapsDB.getConnection() >> oma.getDatabase(Mixing.DEFAULT_REALM).getConnection()

        def newOMA = new OMA()
        newOMA.mixing = oma.mixing
        newOMA.schema = Mock(Schema)
        newOMA.schema.getDatabase(Mixing.DEFAULT_REALM) >> noCapsDB

        SmartQuery<SmartQueryTestEntity> qry = newOMA.select(SmartQueryTestEntity.class)
                                                     .skip(1).limit(2).orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        when:
        def result = qry.queryList()
        then:
        result.stream().map({ x -> x.getValue() } as Function).collect(Collectors.toList()) == ["Hello", "World"]
    }

    def "automatic joins work when sorting by a referenced field"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestChildEntity.class)
                                                       .
                orderAsc(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME))
        when:
        def result = qry.queryList()
        then:
        result.stream().map({ x -> x.getName() } as Function).collect(Collectors.toList()) == ["Child 1", "Child 2"]
    }

    def "automatic joins work when fetching a referenced field"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestChildEntity.class)
                                                       .
                fields(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME))
                                                       .
                orderAsc(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME))
        when:
        def result = qry.queryList()
        then:
        result.stream().
                map({ x -> x.getParent().fetchValue().getName() } as Function).
                collect(Collectors.toList()) == ["Parent 1", "Parent 2"]
    }

    def "ids are propertly propagated in join fetches "() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestChildEntity.class)
                                                       .
                fields(
                        SmartQueryTestChildEntity.PARENT,
                        SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME))
        when:
        def result = qry.queryList()
        then:
        result.stream().map({ x -> x.getParent().getId() } as Function).collect(Collectors.toList()) == [1, 2]
    }

    def "automatic joins work when referencing one table in two relations"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestChildEntity.class)
                                                       .
                fields(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME),
                       SmartQueryTestChildEntity.OTHER_PARENT.join(SmartQueryTestParentEntity.NAME))
                                                       .
                orderAsc(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME))
        when:
        def result = qry.queryList()
        then:
        result.stream().
                map({ x -> x.getParent().fetchValue().getName() + x.getOtherParent().fetchValue().getName() } as Function).
                collect(Collectors.toList()) == ["Parent 1Parent 2", "Parent 2Parent 1"]
    }

    def "automatic joins work across several tables"() {
        given:
        SmartQuery<SmartQueryTestChildChildEntity> qry = oma.select(SmartQueryTestChildChildEntity.class)
                                                            .
                fields(
                        SmartQueryTestChildChildEntity.PARENT_CHILD.join(SmartQueryTestChildEntity.PARENT).
                                join(SmartQueryTestParentEntity.NAME))
                                                            .
                orderAsc(
                        SmartQueryTestChildChildEntity.PARENT_CHILD.join(SmartQueryTestChildEntity.PARENT).
                                join(SmartQueryTestParentEntity.NAME))
        when:
        def result = qry.queryList()
        then:
        result.stream().
                map({ x -> x.getParentChild().fetchValue().getParent().fetchValue().getName() } as Function).
                collect(Collectors.toList()) == ["Parent 1"]
    }

    def "exists works when referencing a child entity"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestParentEntity.class).
                where(
                        OMA.FILTERS.existsIn(
                                SmartQueryTestParentEntity.ID,
                                SmartQueryTestChildEntity.class,
                                SmartQueryTestChildEntity.PARENT))
        when:
        def result = qry.queryList()
        then:
        result.stream().map({ x -> x.getName() } as Function).collect(Collectors.toList()) == ["Parent 1", "Parent 2"]
    }

    def "exists works when referencing a child entity with constraints"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestParentEntity.class).
                where(OMA.FILTERS.existsIn(
                        SmartQueryTestParentEntity.ID,
                        SmartQueryTestChildEntity.class,
                        SmartQueryTestChildEntity.PARENT).
                              where(OMA.FILTERS.eq(SmartQueryTestChildEntity.NAME, "Child 1")))
        when:
        def result = qry.queryList()
        then:
        result.stream().map({ x -> x.getName() } as Function).collect(Collectors.toList()) == ["Parent 1"]
    }

    def "exists works when inverted"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestParentEntity.class).
                where(OMA.FILTERS.not(OMA.FILTERS.existsIn(
                        SmartQueryTestParentEntity.ID,
                        SmartQueryTestChildEntity.class,
                        SmartQueryTestChildEntity.PARENT).
                                              where(OMA.FILTERS.eq(SmartQueryTestChildEntity.NAME, "Child 1"))))
        when:
        def result = qry.queryList()
        then:
        result.stream().map({ x -> x.getName() } as Function).collect(Collectors.toList()) == ["Parent 2"]
    }

    def "copy of query does also copy fields"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                                                  .fields(SmartQueryTestEntity.TEST_NUMBER, SmartQueryTestEntity.VALUE)
                                                  .copy()
                                                  .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        when:
        def result = qry.queryList()
        then:
        result.stream().
                map({ x -> x.getValue() } as Function).
                collect(Collectors.toList()) == ["Test", "Hello", "World"]
    }

    def "select non existant entity ref"() {
        given:
        TestEntityWithNullRef testChild = new TestEntityWithNullRef()
        testChild.setName("bliblablub")

        oma.update(testChild)

        when:
        def result = oma.select(TestEntityWithNullRef.class)
                        .fields(TestEntityWithNullRef.NAME,
                                TestEntityWithNullRef.PARENT.join(SmartQueryTestParentEntity.NAME),
                                TestEntityWithNullRef.PARENT.join(SmartQueryTestParentEntity.ID))
                        .eq(TestEntityWithNullRef.ID, testChild.getId()).queryList()

        and:
        TestEntityWithNullRef found = result.get(0)

        then:
        found.getParent().isEmpty()
    }

    def "select existing entity ref with automatic id fetching"() {
        given:
        SmartQueryTestParentEntity parent = new SmartQueryTestParentEntity()
        parent.setName("Parent 3")
        oma.update(parent)
        and:
        TestEntityWithNullRef testChild = new TestEntityWithNullRef()
        testChild.setName("bliblablub")
        testChild.getParent().setValue(parent)

        oma.update(testChild)

        when:
        def result = oma.select(TestEntityWithNullRef.class)
                        .fields(TestEntityWithNullRef.NAME,
                                SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME))
                        .eq(TestEntityWithNullRef.ID, testChild.getId()).queryList()

        and:
        TestEntityWithNullRef found = result.get(0)

        then:
        found.getParent().isFilled()
        and:
        !found.getParent().fetchValue().isNew()
        and:
        Strings.isFilled(found.getParent().fetchValue().getName())
    }

    def "select a entity with attached mixin by value in mixin"() {
        given:
        SmartQueryTestMixinEntity mixinEntity = new SmartQueryTestMixinEntity()
        mixinEntity.setValue("testvalue1")
        SmartQueryTestMixinEntityMixin mixinEntityMixin = mixinEntity.as(SmartQueryTestMixinEntityMixin.class)
        mixinEntityMixin.setMixinValue("mixinvalue1")
        oma.update(mixinEntity)
        when:
        SmartQueryTestMixinEntity entity = oma.select(SmartQueryTestMixinEntity.class).
                eq(
                        SmartQueryTestMixinEntityMixin.MIXIN_VALUE.inMixin(SmartQueryTestMixinEntityMixin.class),
                        "mixinvalue1").
                queryFirst()
        then:
        !mixinEntity.isNew()
        and:
        mixinEntity.getId() == entity.getId()
    }

    @Scope(Scope.SCOPE_NIGHTLY)
    def "selecting over 1000 entities in queryList throws an exception"() {
        given:
        oma.select(ListTestEntity.class).delete()
        and:
        for (int i = 0; i < 1001; i++) {
            def entityToCreate = new ListTestEntity()
            entityToCreate.setCounter(i)
            oma.update(entityToCreate)
        }
        when:
        oma.select(ListTestEntity.class).queryList()
        then:
        thrown(HandledException)
    }
}
