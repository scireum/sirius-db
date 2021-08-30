/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.jdbc.DataTypesEntity;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.Numeric;
import sirius.kernel.commons.Amount;

public class SQLDefaultValuesEntity extends SQLEntity {

    public static final Mapping PRIMITIVE_BOOLEAN = Mapping.named("primitiveBoolean");
    private boolean primitiveBoolean;

    public static final Mapping PRIMITIVE_BOOLEAN_TRUE = Mapping.named("primitiveBooleanTrue");
    private boolean primitiveBooleanTrue = true;

    public static final Mapping BOOLEAN_OBJECT = Mapping.named("booleanObject");
    private Boolean booleanObject;

    public static final Mapping PRIMITIVE_INT = Mapping.named("primitiveInt");
    private int primitiveInt;

    public static final Mapping PRIMITIVE_INT_WITH_VALUE = Mapping.named("primitiveIntWithValue");
    private int primitiveIntWithValue = 50;

    public static final Mapping AMOUNT_WITH_VALUE = Mapping.named("amountWithValue");
    @Numeric(precision = 15, scale = 3)
    private Amount amountWithValue = Amount.ONE_HUNDRED;

    public static final Mapping AMOUNT_ZERO = Mapping.named("amountZero");
    @Numeric(precision = 15, scale = 3)
    private Amount amountZero = Amount.ZERO;

    public static final Mapping AMOUNT_NOTHING = Mapping.named("amountNothing");
    @Numeric(precision = 15, scale = 3)
    private Amount amountNothing = Amount.NOTHING;

    public static final Mapping STRING = Mapping.named("string");
    @Length(15)
    private String string;

    public static final Mapping EMPTY_STRING = Mapping.named("emptyString");
    @Length(15)
    private String emptyString = "";

    public static final Mapping FILLED_STRING = Mapping.named("filledString");
    @Length(15)
    private String filledString = "STRING";

    public static final Mapping ENUM_TEST = Mapping.named("enumTest");
    private DataTypesEntity.TestEnum enumTest;

    public static final Mapping ENUM_WITH_VALUE = Mapping.named("enumWithValue");
    private DataTypesEntity.TestEnum enumWithValue = DataTypesEntity.TestEnum.Test2;

    public boolean isPrimitiveBoolean() {
        return primitiveBoolean;
    }

    public void setPrimitiveBoolean(boolean primitiveBoolean) {
        this.primitiveBoolean = primitiveBoolean;
    }

    public boolean isPrimitiveBooleanTrue() {
        return primitiveBooleanTrue;
    }

    public void setPrimitiveBooleanTrue(boolean primitiveBooleanTrue) {
        this.primitiveBooleanTrue = primitiveBooleanTrue;
    }

    public Boolean getBooleanObject() {
        return booleanObject;
    }

    public void setBooleanObject(Boolean booleanObject) {
        this.booleanObject = booleanObject;
    }

    public int getPrimitiveInt() {
        return primitiveInt;
    }

    public void setPrimitiveInt(int primitiveInt) {
        this.primitiveInt = primitiveInt;
    }

    public int getPrimitiveIntWithValue() {
        return primitiveIntWithValue;
    }

    public void setPrimitiveIntWithValue(int primitiveIntWithValue) {
        this.primitiveIntWithValue = primitiveIntWithValue;
    }

    public Amount getAmountWithValue() {
        return amountWithValue;
    }

    public void setAmountWithValue(Amount amountWithValue) {
        this.amountWithValue = amountWithValue;
    }

    public Amount getAmountZero() {
        return amountZero;
    }

    public void setAmountZero(Amount amountZero) {
        this.amountZero = amountZero;
    }

    public Amount getAmountNothing() {
        return amountNothing;
    }

    public void setAmountNothing(Amount amountNothing) {
        this.amountNothing = amountNothing;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public String getEmptyString() {
        return emptyString;
    }

    public void setEmptyString(String emptyString) {
        this.emptyString = emptyString;
    }

    public String getFilledString() {
        return filledString;
    }

    public void setFilledString(String filledString) {
        this.filledString = filledString;
    }

    public DataTypesEntity.TestEnum getEnumTest() {
        return enumTest;
    }

    public void setEnumTest(DataTypesEntity.TestEnum enumTest) {
        this.enumTest = enumTest;
    }

    public DataTypesEntity.TestEnum getEnumWithValue() {
        return enumWithValue;
    }

    public void setEnumWithValue(DataTypesEntity.TestEnum enumWithValue) {
        this.enumWithValue = enumWithValue;
    }
}
