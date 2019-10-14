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

    def "a guarded query updates the expected entities"() {
        given:
        UpdateStatementTestEntity e1 = new UpdateStatementTestEntity()
        e1.setTestNumber(1)
        e1.setValue("2")
        oma.update(e1)
        and:
        UpdateStatementTestEntity e2 = new UpdateStatementTestEntity()
        e2.setTestNumber(3)
        e2.setValue("4")
        oma.update(e2)
        when:
        int changes = oma.
                updateStatement(UpdateStatementTestEntity.class).
                set(UpdateStatementTestEntity.VALUE, "5").
                where(UpdateStatementTestEntity.TEST_NUMBER, 1).
                executeUpdate()
        then: "One entity was changed"
        changes == 1
        and: "e1 was update"
        oma.refreshOrFail(e1).getValue() == "5"
        and: "e2 wasn't"
        oma.refreshOrFail(e2).getValue() == "4"
    }

    def "a guarded query reports illegal use (set after where)"() {
        when:
        oma.updateStatement(UpdateStatementTestEntity.class).
                where(UpdateStatementTestEntity.TEST_NUMBER, 1).
                set(UpdateStatementTestEntity.VALUE, "5").
                executeUpdate()
        then:
        thrown(IllegalStateException)
    }

    def "a guarded query ignores an empty update without errors"() {
        when:
        int changes = oma.updateStatement(UpdateStatementTestEntity.class).
                where(UpdateStatementTestEntity.TEST_NUMBER, 1).
                executeUpdate()
        then: "Nothing really happens"
        changes == 0
    }

    def "a guarded query detects and reports join columns as errors"() {
        when:
        oma.updateStatement(SmartQueryTestChildEntity.class).
                set(SmartQueryTestChildEntity.NAME, "X").
                where(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME), 1).
                executeUpdate()
        then:
        thrown(IllegalArgumentException)
    }

}
