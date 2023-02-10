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
 * Test entity for SmartQuerySpec concerning distinct and not distinct counts
 */
@ComplexDelete(false)
public class SmartQueryTestCountEntity extends SQLEntity {

    public static final Mapping FIELD_ONE = Mapping.named("fieldOne");
    @Length(50)
    private String fieldOne;

    public static final Mapping FIELD_TWO = Mapping.named("fieldTwo");
    @Length(50)
    private String fieldTwo;

    public String getFieldOne() {
        return fieldOne;
    }

    public void setFieldOne(String fieldOne) {
        this.fieldOne = fieldOne;
    }

    public String getFieldTwo() {
        return fieldTwo;
    }

    public void setFieldTwo(String fieldTwo) {
        this.fieldTwo = fieldTwo;
    }
}
