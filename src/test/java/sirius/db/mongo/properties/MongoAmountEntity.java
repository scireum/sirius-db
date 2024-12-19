/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Numeric;
import sirius.db.mongo.MongoEntity;
import sirius.kernel.commons.Amount;

public class MongoAmountEntity extends MongoEntity {

    public static final int AMOUNT_SCALE = 3;

    public static final Mapping TEST_AMOUNT = Mapping.named("testAmount");
    private Amount testAmount = Amount.NOTHING;

    public static final Mapping SCALED_AMOUNT = Mapping.named("scaledAmount");
    @NullAllowed
    @Numeric(precision = 20, scale = AMOUNT_SCALE)
    private Amount scaledAmount = Amount.NOTHING;

    public Amount getTestAmount() {
        return testAmount;
    }

    public void setTestAmount(Amount testAmount) {
        this.testAmount = testAmount;
    }

    public Amount getScaledAmount() {
        return scaledAmount;
    }

    public void setScaledAmount(Amount scaledAmount) {
        this.scaledAmount = scaledAmount;
    }
}
