/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import sirius.db.mongo.Mango
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Tuple
import sirius.kernel.di.std.Part

class MongoMultiPointSpec extends BaseSpecification {

    @Part
    private static Mango mango

            def "read/write multipoint works"() {
        setup:
        MongoMultiPointEntity entity = new MongoMultiPointEntity()
        when:
        mango.update(entity)
        then:
        entity.getLocations().isEmpty()
        when:
        List<Tuple<Double, Double>> coords = [Tuple.create(48.81734d, 9.376294d), Tuple.create(48.823356d, 9.424718d)]
        entity.getLocations().addAll(coords)
        mango.update(entity)
        then:
        entity.getLocations().size() == 2
        when:
        entity = mango.refreshOrFail(entity)
        then:
        entity.getLocations().size() == 2
    }
}
