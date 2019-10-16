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

class DeleteStatementSpec extends BaseSpecification {

    @Part
    private static OMA oma;

    def "a delete statement deletes the expected entities"() {
        given:
        GeneratedStatementTestEntity e1 = new GeneratedStatementTestEntity()
        e1.setTestNumber(4711)
        e1.setValue("2")
        oma.update(e1)
        and:
        GeneratedStatementTestEntity e2 = new GeneratedStatementTestEntity()
        e2.setTestNumber(4712)
        e2.setValue("4")
        oma.update(e2)
        when:
        int changes = oma.
                deleteStatement(GeneratedStatementTestEntity.class).
                where(GeneratedStatementTestEntity.TEST_NUMBER, 4711).
                executeUpdate()
        then: "One entits was removed"
        changes == 1
        and: "e1 was removed"
        !oma.find(GeneratedStatementTestEntity.class, e1.getId()).isPresent()
        and: "e2 wasn't"
        oma.find(GeneratedStatementTestEntity.class, e2.getId()).isPresent()
    }


    def "a delete statement deletes all entities without errors"() {
        given:
        GeneratedStatementTestEntity e1 = new GeneratedStatementTestEntity()
        e1.setTestNumber(4711)
        e1.setValue("2")
        oma.update(e1)
        when:
        oma.deleteStatement(GeneratedStatementTestEntity.class).
                executeUpdate()
        then: "All entities are removed"
        oma.select(GeneratedStatementTestEntity.class).count() == 0
    }

}
