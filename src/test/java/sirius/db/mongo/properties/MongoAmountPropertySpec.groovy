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
import sirius.kernel.commons.Amount
import sirius.kernel.di.std.Part

class MongoAmountPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

    def "read/write of amount fields works"() {
        setup:
        MongoAmountEntity obj = new MongoAmountEntity()
        obj.setTestAmount(Amount.of(-3.77d))
        mango.update(obj)
        when:
        obj = mango.refreshOrFail(obj)
        then:
        obj.getTestAmount() == Amount.of(-3.77d)
    }
}
