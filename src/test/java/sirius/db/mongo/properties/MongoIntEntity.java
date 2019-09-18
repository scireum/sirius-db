/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.MaxValue;
import sirius.db.mixing.annotations.MinValue;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Positive;
import sirius.db.mongo.MongoEntity;

public class MongoIntEntity extends MongoEntity {

    public static final Mapping TEST_INT_PRIMITIVE = Mapping.named("testIntPrimitive");
    private int testIntPrimitive;

    public static final Mapping TEST_INT_OBJECT = Mapping.named("testIntObject");
    private Integer testIntObject;

    public static final Mapping TEST_INT_POSITIVE = Mapping.named("testIntPositive");
    @Positive
    @NullAllowed
    private Integer testIntPositive;

    public static final Mapping TEST_INT_POSITIVE_WITH_ZERO = Mapping.named("testIntPositiveWithZero");
    @Positive(includeZero = true)
    @NullAllowed
    private Integer testIntPositiveWithZero;

    public static final Mapping TEST_INT_MAX_HUNDRED = Mapping.named("testIntMaxHundred");
    @MaxValue(100)
    @NullAllowed
    private Integer testIntMaxHundred;

    public static final Mapping TEST_INT_MIN_HUNDRED = Mapping.named("testIntMinHundred");
    @MinValue(100)
    @NullAllowed
    private Integer testIntMinHundred;

    public static final Mapping TEST_INT_TWENTYS = Mapping.named("testIntTwentys");
    @MinValue(20)
    @MaxValue(9)
    @NullAllowed
    private Integer testIntTwentys;

    public int getTestIntPrimitive() {
        return testIntPrimitive;
    }

    public void setTestIntPrimitive(int testIntPrimitive) {
        this.testIntPrimitive = testIntPrimitive;
    }

    public Integer getTestIntObject() {
        return testIntObject;
    }

    public void setTestIntObject(Integer testIntObject) {
        this.testIntObject = testIntObject;
    }

    public Integer getTestIntPositive() {
        return testIntPositive;
    }

    public void setTestIntPositive(Integer testIntPositive) {
        this.testIntPositive = testIntPositive;
    }

    public Integer getTestIntPositiveWithZero() {
        return testIntPositiveWithZero;
    }

    public void setTestIntPositiveWithZero(Integer testIntPositiveWithZero) {
        this.testIntPositiveWithZero = testIntPositiveWithZero;
    }

    public Integer getTestIntMaxHundred() {
        return testIntMaxHundred;
    }

    public void setTestIntMaxHundred(Integer testIntMaxHundred) {
        this.testIntMaxHundred = testIntMaxHundred;
    }

    public Integer getTestIntMinHundred() {
        return testIntMinHundred;
    }

    public void setTestIntMinHundred(Integer testIntMinHundred) {
        this.testIntMinHundred = testIntMinHundred;
    }

    public Integer getTestIntTwentys() {
        return testIntTwentys;
    }

    public void setTestIntTwentys(Integer testIntTwentys) {
        this.testIntTwentys = testIntTwentys;
    }
}
