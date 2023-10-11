/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.ValidatedBy;
import sirius.db.mongo.validators.StringTestPropertyValidator;

public class ValidatedByTestEntity extends MongoEntity {

    public static final Mapping STRICT_STRING_TEST = Mapping.named("strictStringTest");
    @NullAllowed
    @ValidatedBy(StringTestPropertyValidator.class)
    private String strictStringTest;

    public static final Mapping LENIENT_STRING_TEST = Mapping.named("lenientStringTest");
    @NullAllowed
    @ValidatedBy(value = StringTestPropertyValidator.class, strictValidation = false)
    private String lenientStringTest;

    public static final Mapping UNVALIDATED_STRING_TEST = Mapping.named("unvalidatedStringTest");
    @NullAllowed
    private String unvalidatedStringTest;

    public String getStrictStringTest() {
        return strictStringTest;
    }

    public void setStrictStringTest(String strictStringTest) {
        this.strictStringTest = strictStringTest;
    }

    public String getLenientStringTest() {
        return lenientStringTest;
    }

    public void setLenientStringTest(String lenientStringTest) {
        this.lenientStringTest = lenientStringTest;
    }

    public String getUnvalidatedStringTest() {
        return unvalidatedStringTest;
    }

    public void setUnvalidatedStringTest(String unvalidatedStringTest) {
        this.unvalidatedStringTest = unvalidatedStringTest;
    }
}
