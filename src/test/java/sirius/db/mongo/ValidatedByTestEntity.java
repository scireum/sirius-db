/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.annotations.ValidatedBy;
import sirius.db.mongo.validators.StringTestPropertyValidator;

public class ValidatedByTestEntity extends MongoEntity {

    @ValidatedBy(StringTestPropertyValidator.class)
    private String stringTest;

    public String getStringTest() {
        return stringTest;
    }

    public void setStringTest(String stringTest) {
        this.stringTest = stringTest;
    }
}
