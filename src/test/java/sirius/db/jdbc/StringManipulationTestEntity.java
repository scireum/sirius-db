/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Index;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.LowerCase;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Trim;
import sirius.db.mixing.annotations.UpperCase;

public class StringManipulationTestEntity extends SQLEntity {

    public static final Mapping TRIMMED = Mapping.named("trimmed");
    @NullAllowed
    @Trim
    private String trimmed;

    public static final Mapping LOWER = Mapping.named("lower");
    @NullAllowed
    @LowerCase
    private String lower;

    public static final Mapping UPPER = Mapping.named("upper");
    @NullAllowed
    @UpperCase
    private String upper;

    public static final Mapping TRIMMED_LOWER = Mapping.named("trimmedLower");
    @NullAllowed
    @LowerCase
    @Trim
    private String trimmedLower;

    public static final Mapping TRIMMED_UPPER = Mapping.named("trimmedUpper");
    @NullAllowed
    @UpperCase
    @Trim
    private String trimmedUpper;

    public String getTrimmed() {
        return trimmed;
    }

    public void setTrimmed(String trimmed) {
        this.trimmed = trimmed;
    }

    public String getLower() {
        return lower;
    }

    public void setLower(String lower) {
        this.lower = lower;
    }

    public String getUpper() {
        return upper;
    }

    public void setUpper(String upper) {
        this.upper = upper;
    }

    public String getTrimmedLower() {
        return trimmedLower;
    }

    public void setTrimmedLower(String trimmedLower) {
        this.trimmedLower = trimmedLower;
    }

    public String getTrimmedUpper() {
        return trimmedUpper;
    }

    public void setTrimmedUpper(String trimmedUpper) {
        this.trimmedUpper = trimmedUpper;
    }
}
