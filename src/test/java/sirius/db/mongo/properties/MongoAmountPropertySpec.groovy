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

    private Amount saveAndRead(Amount value) {
        MongoAmountEntity obj = new MongoAmountEntity()
        obj.setTestAmount(value)
        mango.update(obj)
        obj = mango.refreshOrFail(obj)
        return obj.getTestAmount()
    }

    def "read/write of amount fields works"() {
        expect:
        saveAndRead(Amount.of(value)) == Amount.of(value)

        where:
        value << [-3.77d, Double.MAX_VALUE, 0.00001d, -0.00001d]
    }
}
