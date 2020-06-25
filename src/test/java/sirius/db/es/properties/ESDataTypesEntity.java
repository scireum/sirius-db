/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import sirius.db.es.ElasticEntity;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Ordinal;
import sirius.kernel.commons.Amount;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class ESDataTypesEntity extends ElasticEntity {

    public enum TestEnum {
        Test1, Test2;

        @Override
        public String toString() {
            return name() + name().length();
        }
    }

    private static final Mapping LONG_VALUE = Mapping.named("longValue");
    @NullAllowed
    private Long longValue;

    private static final Mapping INT_VALUE = Mapping.named("intValue");
    @NullAllowed
    private Integer intValue;

    private static final Mapping STRING_VALUE = Mapping.named("stringValue");
    @Length(255)
    @NullAllowed
    private String stringValue;

    private static final Mapping AMOUNT_VALUE = Mapping.named("amountValue");
    @NullAllowed
    private Amount amountValue = Amount.NOTHING;

    private static final Mapping LOCALDATE_VALUE = Mapping.named("localDateValue");
    @NullAllowed
    private LocalDate localDateValue;

    private static final Mapping LOCALDATETIME_VALUE = Mapping.named("localDateTimeValue");
    @NullAllowed
    private LocalDateTime localDateTimeValue;

    private static final Mapping BOOL_VALUE = Mapping.named("boolValue");
    @NullAllowed
    private Boolean boolValue;

    private static final Mapping ENUM_VALUE = Mapping.named("enumValue");
    @NullAllowed
    private TestEnum enumValue;

    @NullAllowed
    @Ordinal
    private TestEnum enumValue2;

    private int intValue2 = 5;

    private long longValue2 = Long.MAX_VALUE;

    public Long getLongValue() {
        return longValue;
    }

    public void setLongValue(Long longValue) {
        this.longValue = longValue;
    }

    public Integer getIntValue() {
        return intValue;
    }

    public void setIntValue(Integer intValue) {
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

    public LocalDateTime getLocalDateTimeValue() {
        return localDateTimeValue;
    }

    public void setLocalDateTimeValue(LocalDateTime localDateTimeValue) {
        this.localDateTimeValue = localDateTimeValue;
    }

    public TestEnum getTestEnum() {
        return enumValue;
    }

    public void setTestEnum(TestEnum enumValue) {
        this.enumValue = enumValue;
    }

    public TestEnum getTestEnum2() {
        return enumValue2;
    }

    public void setTestEnum2(TestEnum enumValue2) {
        this.enumValue2 = enumValue2;
    }

    public long getLongValue2() {
        return longValue2;
    }

    public void setLongValue2(long longValue2) {
        this.longValue2 = longValue2;
    }

    public int getIntValue2() {
        return intValue2;
    }

    public void setIntValue2(int intValue2) {
        this.intValue2 = intValue2;
    }
}
