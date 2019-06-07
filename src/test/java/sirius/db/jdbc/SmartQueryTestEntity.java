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
 * Testentity for SmartQuerySpec
 */
@ComplexDelete(false)
public class SmartQueryTestEntity extends SQLEntity {

    @Length(50)
    private String value;
    public static final Mapping VALUE = Mapping.named("value");

    private int testNumber;
    public static final Mapping TEST_NUMBER = Mapping.named("testNumber");

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
