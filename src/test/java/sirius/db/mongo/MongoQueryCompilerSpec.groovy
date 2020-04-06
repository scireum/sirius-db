/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import sirius.db.mixing.Mixing
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class MongoQueryCompilerSpec extends BaseSpecification {

    @Part
    private static Mango mango

    @Part
    private static Mixing mixing

    def "listField:- generates an isEmptyListConstraint"() {
        when: "We create an example entity with an empty list (superPowers)"
        MangoTestEntity testEntity = new MangoTestEntity()
        testEntity.firstname = "Compiler"
        testEntity.lastname = "Test"
        and:
        mango.update(testEntity)
        and: "And we search for exactly that entity while enforcing an empty list."
        MangoTestEntity queryResult = mango.
                select(MangoTestEntity.class).
                where(QueryBuilder.FILTERS.queryString(mixing.getDescriptor(MangoTestEntity.class),
                                                       "id:${testEntity.getIdAsString()} superPowers:-")).
                queryFirst()
        then: "This entity is found..."
        queryResult != null
        queryResult.getId() == testEntity.getId()
        and: "And filtering on a real value within the list still also works..."
        !mango.
                select(MangoTestEntity.class).
                where(QueryBuilder.FILTERS.queryString(mixing.getDescriptor(MangoTestEntity.class),
                                                       "id:${testEntity.getIdAsString()} superPowers:WriteCode")).
                exists()
    }

}
