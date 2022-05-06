/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.annotations.DefaultValue;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.SkipDefaultValue;
import sirius.db.mixing.types.StringList;
import sirius.db.mixing.types.StringMap;

public class SkipDefaultTestEntity extends MongoEntity {

    @SkipDefaultValue
    @NullAllowed
    private String stringTest;

    @SkipDefaultValue
    private boolean boolTest;

    @SkipDefaultValue
    private final StringList listTest = new StringList();

    @SkipDefaultValue
    private final StringMap mapTest = new StringMap();

    @SkipDefaultValue
    @DefaultValue("100")
    private Integer integerWithDefault = 100;

    public String getStringTest() {
        return stringTest;
    }

    public void setStringTest(String stringTest) {
        this.stringTest = stringTest;
    }

    public boolean isBoolTest() {
        return boolTest;
    }

    public void setBoolTest(boolean boolTest) {
        this.boolTest = boolTest;
    }

    public StringList getListTest() {
        return listTest;
    }

    public StringMap getMapTest() {
        return mapTest;
    }

    public Integer getIntegerWithDefault() {
        return integerWithDefault;
    }

    public void setIntegerWithDefault(Integer integerWithDefault) {
        this.integerWithDefault = integerWithDefault;
    }
}
