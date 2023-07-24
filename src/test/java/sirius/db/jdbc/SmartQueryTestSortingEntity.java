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
import sirius.db.mixing.annotations.NullAllowed;

import javax.annotation.Nullable;

/**
 * Testentity for SmartQuerySpec
 */
@ComplexDelete(false)
public class SmartQueryTestSortingEntity extends SQLEntity {

    @Length(50)
    @NullAllowed
    private String valueOne;
    public static final Mapping VALUE_ONE = Mapping.named("valueOne");

    @Length(50)
    @NullAllowed
    private String valueTwo;
    public static final Mapping VALUE_TWO = Mapping.named("valueTwo");

    public String getValueOne() {
        return valueOne;
    }

    public void setValueOne(String valueOne) {
        this.valueOne = valueOne;
    }

    public String getValueTwo() {
        return valueTwo;
    }

    public void setValueTwo(String valueTwo) {
        this.valueTwo = valueTwo;
    }
}
