/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.Length;

/**
 * Testentity for SmartQuerySpec
 */
public class SmartQueryTestEntity extends Entity {

    @Length(length = 50)
    private String value;
    public static final Column VALUE = Column.named("value");

    private int testNumber;
    public static final Column TEST_NUMBER = Column.named("testNumber");

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getTestNumber() {
        return testNumber;
    }

    public void setTestNumber(int testNumber) {
        this.testNumber = testNumber;
    }
}
