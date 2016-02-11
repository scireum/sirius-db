/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.db.jdbc.Database
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.mixing.OMA
import sirius.mixing.Schema
import sirius.mixing.SmartQuery
import sirius.mixing.constraints.FieldOperator
import spock.lang.Stepwise

import java.util.function.Function
import java.util.stream.Collectors

@Stepwise
class SmartQuerySpec extends BaseSpecification {

    @Part
    static OMA oma;

    def setupSpec() {
        fillSmartQueryTestEntity();
        fillSmartQueryTestChildAndParentEntity();
    }

    private void fillSmartQueryTestEntity() {
        SmartQueryTestEntity e = new SmartQueryTestEntity();
        e.setValue("Test");
        e.setTestNumber(1);
        oma.update(e);
        e = new SmartQueryTestEntity();
        e.setValue("Hello");
        e.setTestNumber(2);
        oma.update(e);
        e = new SmartQueryTestEntity();
        e.setValue("World");
        e.setTestNumber(3);
        oma.update(e);
    }

    private void fillSmartQueryTestChildAndParentEntity() {
        SmartQueryTestParentEntity p1 = new SmartQueryTestParentEntity();
        p1.setName("Parent 1");
        oma.update(p1);
        SmartQueryTestParentEntity p2 = new SmartQueryTestParentEntity();
        p2.setName("Parent 2");
        oma.update(p2);
        SmartQueryTestChildEntity c = new SmartQueryTestChildEntity();
        c.setName("Child 1")
        c.getParent().setId(p1.getId());
        c.getOtherParent().setId(p2.getId());
        oma.update(c);
        c = new SmartQueryTestChildEntity();
        c.setName("Child 2")
        c.getParent().setId(p2.getId());
        c.getOtherParent().setId(p1.getId());
        oma.update(c);
    }

    def "queryList returns all entities"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER);
        when:
        def result = qry.queryList();
        then:
        result.stream().map({ x -> x.getValue() } as Function).collect(Collectors.toList()) == ["Test", "Hello", "World"]
    }

    def "count returns the number of entity"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER);
        when:
        def result = qry.count();
        then:
        result == 3
    }

    def "exists returns a correct value"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER);
        when:
        def result = qry.exists();
        then:
        result == true
    }

    def "queryFirst returns the first entity"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER);
        when:
        def result = qry.queryFirst();
        then:
        result.getValue() == "Test"
    }

    def "first returns the first entity"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER);
        when:
        def result = qry.first();
        then:
        result.get().getValue() == "Test"
    }

    def "queryFirst returns null for an empty result"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                .where(FieldOperator.on(SmartQueryTestEntity.VALUE).equal("xxx"));
        when:
        def result = qry.queryFirst();
        then:
        result == null
    }

    def "first returns an empty optional for an empty result"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                .where(FieldOperator.on(SmartQueryTestEntity.VALUE).equal("xxx"));
        when:
        def result = qry.first();
        then:
        !result.isPresent()
    }

    def "limit works on a plain list"() {
        given:
        SmartQuery<SmartQueryTestEntity> qry = oma.select(SmartQueryTestEntity.class)
                .skip(1).limit(2).orderAsc(SmartQueryTestEntity.TEST_NUMBER);
        when:
        def result = qry.queryList();
        then:
        result.stream().map({ x -> x.getValue() } as Function).collect(Collectors.toList()) == ["Hello", "World"]
    }

    def "limit works on a plain list (skipping native LIMIT)"() {
        given:
        def noCapsDB = Mock(Database)
        noCapsDB.hasCapability(_) >> false
        noCapsDB.getConnection() >> oma.getDatabase().getConnection()

        def newOMA = new OMA();
        newOMA.schema = Mock(Schema);
        newOMA.schema.getDatabase() >> noCapsDB

        SmartQuery<SmartQueryTestEntity> qry = newOMA.select(SmartQueryTestEntity.class)
                .skip(1).limit(2).orderAsc(SmartQueryTestEntity.TEST_NUMBER);
        when:
        def result = qry.queryList();
        then:
        result.stream().map({ x -> x.getValue() } as Function).collect(Collectors.toList()) == ["Hello", "World"]
    }

    def "automatic joins work when sorting by a referenced field"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestChildEntity.class)
                .orderAsc(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME));
        when:
        def result = qry.queryList();
        then:
        result.stream().map({ x -> x.getName() } as Function).collect(Collectors.toList()) == ["Child 1", "Child 2"]
    }

    def "automatic joins work when fetching a referenced field"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestChildEntity.class)
                .fields(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME))
                .orderAsc(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME));
        when:
        def result = qry.queryList();
        then:
        result.stream().map({ x -> x.getParent().getValue().getName() } as Function).collect(Collectors.toList()) == ["Parent 1", "Parent 2"]
    }

    def "automatic joins work when referencing one table in two relations"() {
        given:
        SmartQuery<SmartQueryTestChildEntity> qry = oma.select(SmartQueryTestChildEntity.class)
                .fields(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME),
                        SmartQueryTestChildEntity.OTHER_PARENT.join(SmartQueryTestParentEntity.NAME))
                .orderAsc(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME));
        when:
        def result = qry.queryList();
        then:
        result.stream().map({ x -> x.getParent().getValue().getName() + x.getOtherParent().getValue().getName() } as Function).collect(Collectors.toList()) == ["Parent 1Parent 2", "Parent 2Parent 1"]
    }

}
