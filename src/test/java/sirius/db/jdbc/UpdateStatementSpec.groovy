/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class UpdateStatementSpec extends BaseSpecification {

    @Part
    private static OMA oma;

    def "an update statement updates the expected entities"() {
        given:
        GeneratedStatementTestEntity e1 = new GeneratedStatementTestEntity()
        e1.setTestNumber(1)
        e1.setValue("2")
        oma.update(e1)
        and:
        GeneratedStatementTestEntity e2 = new GeneratedStatementTestEntity()
        e2.setTestNumber(3)
        e2.setValue("4")
        oma.update(e2)
        when:
        int changes = oma.
                updateStatement(GeneratedStatementTestEntity.class).
                set(GeneratedStatementTestEntity.VALUE, "5").
                where(GeneratedStatementTestEntity.TEST_NUMBER, 1).
                executeUpdate()
        then: "One entity was changed"
        changes == 1
        and: "e1 was update"
        oma.refreshOrFail(e1).getValue() == "5"
        and: "e2 wasn't"
        oma.refreshOrFail(e2).getValue() == "4"
    }

    def "a update statement reports illegal use (set after where)"() {
        when:
        oma.updateStatement(GeneratedStatementTestEntity.class).
                where(GeneratedStatementTestEntity.TEST_NUMBER, 1).
                set(GeneratedStatementTestEntity.VALUE, "5").
                executeUpdate()
        then:
        thrown(IllegalStateException)
    }

    def "a update statement ignores an empty update without errors"() {
        when:
        int changes = oma.updateStatement(GeneratedStatementTestEntity.class).
                where(GeneratedStatementTestEntity.TEST_NUMBER, 1).
                executeUpdate()
        then: "Nothing really happens"
        changes == 0
    }

    def "a update statement detects and reports join columns as errors"() {
        when:
        oma.updateStatement(SmartQueryTestChildEntity.class).
                set(SmartQueryTestChildEntity.NAME, "X").
                where(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME), 1).
                executeUpdate()
        then:
        thrown(IllegalArgumentException)
    }

}
