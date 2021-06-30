/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.ComplexDelete;
import sirius.db.mixing.annotations.Length;

/**
 * Testentity for SmartQuerySpec with many items in the table
 */
@ComplexDelete(false)
public class SmartQueryTestLargeTableEntity extends SQLEntity {
    public static final Mapping TEST_NUMBER = Mapping.named("testNumber");
    private int testNumber;

    public int getTestNumber() {
        return testNumber;
    }

    public void setTestNumber(int testNumber) {
        this.testNumber = testNumber;
    }
}
