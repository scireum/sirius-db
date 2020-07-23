/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.Mapping;

public class MangoAggregationsTestEntity extends MongoEntity {

    public static final Mapping TEST_INT = Mapping.named("testInt");
    private int testInt;

    public int getTestInt() {
        return testInt;
    }

    public void setTestInt(int testInt) {
        this.testInt = testInt;
    }
}
