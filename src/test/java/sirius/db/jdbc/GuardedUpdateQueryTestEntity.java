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
import sirius.db.mixing.types.BaseEntityRef;

import javax.annotation.Nullable;

/**
 * Testentity for GuardedUpdateQuerySpec
 */
@ComplexDelete(false)
public class GuardedUpdateQueryTestEntity extends SQLEntity {

    public static final Mapping VALUE = Mapping.named("value");
    @Length(50)
    private String value;

    public static final Mapping TEST_NUMBER = Mapping.named("testNumber");
    private int testNumber;

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
