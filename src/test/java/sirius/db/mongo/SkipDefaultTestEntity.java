/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.SkipDefaultValue;

public class SkipDefaultTestEntity extends MongoEntity {

    @SkipDefaultValue
    @NullAllowed
    private String stringTest;

    @SkipDefaultValue
    private boolean boolTest;

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
}
