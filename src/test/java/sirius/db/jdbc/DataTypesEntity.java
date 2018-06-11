/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.annotations.DefaultValue;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Numeric;
import sirius.kernel.commons.Amount;

import java.time.LocalDate;
import java.time.LocalTime;

public class DataTypesEntity extends SQLEntity {

    public enum TestEnum {
        Test1, Test2
    }

    @DefaultValue("100")
    @NullAllowed
    private Long longValue;

    @DefaultValue("200")
    @NullAllowed
    private Integer intValue;

    @DefaultValue("test")
    @Length(255)
    @NullAllowed
    private String stringValue;

    @DefaultValue("300")
    @NullAllowed
    @Numeric(precision = 20, scale = 3)
    private Amount amountValue = Amount.NOTHING;

    @DefaultValue("2018-01-01")
    @NullAllowed
    private LocalDate localDateValue;

    @DefaultValue("10:15:30")
    @NullAllowed
    private LocalTime localTimeValue;

    @DefaultValue("true")
    @NullAllowed
    private Boolean boolValue;

    @DefaultValue("Test2")
    @NullAllowed
    private TestEnum enumValue;

    private int intValue2 = 5;

    public long getLongValue() {
        return longValue;
    }

    public void setLongValue(long longValue) {
        this.longValue = longValue;
    }

    public int getIntValue() {
        return intValue;
    }

    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }

    public String getStringValue() {
        return stringValue;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public Amount getAmountValue() {
        return amountValue;
    }

    public void setAmountValue(Amount amountValue) {
        this.amountValue = amountValue;
    }

    public LocalDate getLocalDateValue() {
        return localDateValue;
    }

    public void setLocalDateValue(LocalDate localDateValue) {
        this.localDateValue = localDateValue;
    }

    public Boolean getBoolValue() {
        return boolValue;
    }

    public void setBoolValue(Boolean boolValue) {
        this.boolValue = boolValue;
    }

    public LocalTime getLocalTimeValue() {
        return localTimeValue;
    }

    public void setLocalTimeValue(LocalTime localTimeValue) {
        this.localTimeValue = localTimeValue;
    }

    public TestEnum getTestEnum() {
        return enumValue;
    }

    public void setTestEnum(TestEnum enumValue) {
        this.enumValue = enumValue;
    }
}
