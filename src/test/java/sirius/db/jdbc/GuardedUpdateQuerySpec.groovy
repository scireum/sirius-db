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

class GuardedUpdateQuerySpec extends BaseSpecification {

    @Part
    private static OMA oma;

    def "a guarded query updates the expected entities"() {
        given:
        GuardedUpdateQueryTestEntity e1 = new GuardedUpdateQueryTestEntity()
        e1.setTestNumber(1)
        e1.setValue("2")
        oma.update(e1)
        and:
        GuardedUpdateQueryTestEntity e2 = new GuardedUpdateQueryTestEntity()
        e2.setTestNumber(3)
        e2.setValue("4")
        oma.update(e2)
        when:
        int changes = oma.
                guardedUpdate(GuardedUpdateQueryTestEntity.class).
                set(GuardedUpdateQueryTestEntity.VALUE, "5").
                where(GuardedUpdateQueryTestEntity.TEST_NUMBER, 1).
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
        oma.guardedUpdate(GuardedUpdateQueryTestEntity.class).
                where(GuardedUpdateQueryTestEntity.TEST_NUMBER, 1).
                set(GuardedUpdateQueryTestEntity.VALUE, "5").
                executeUpdate()
        then:
        thrown(IllegalStateException)
    }

    def "a guarded query ignores an empty update without errors"() {
        when:
        int changes = oma.guardedUpdate(GuardedUpdateQueryTestEntity.class).
                where(GuardedUpdateQueryTestEntity.TEST_NUMBER, 1).
                executeUpdate()
        then: "Nothing really happens"
        changes == 0
    }

    def "a guarded query detects and reports join columns as errors"() {
        when:
        oma.guardedUpdate(SmartQueryTestChildEntity.class).
                set(SmartQueryTestChildEntity.NAME, "X").
                where(SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME), 1).
                executeUpdate()
        then:
        thrown(IllegalArgumentException)
    }

}
