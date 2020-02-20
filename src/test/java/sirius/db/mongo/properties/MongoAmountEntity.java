/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mongo.MongoEntity;
import sirius.kernel.commons.Amount;

public class MongoAmountEntity extends MongoEntity {

    public static final Mapping TEST_AMOUNT = Mapping.named("testAmount");
    private Amount testAmount = Amount.NOTHING;

    public Amount getTestAmount() {
        return testAmount;
    }

    public void setTestAmount(Amount testAmount) {
        this.testAmount = testAmount;
    }
}
